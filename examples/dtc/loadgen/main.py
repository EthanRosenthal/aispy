from dataclasses import dataclass
import datetime as dt
import json
import logging
from queue import PriorityQueue
import random
import string
import threading
import time
from typing import Optional, Tuple

from kafka import KafkaProducer
import psycopg2
from psycopg2.extensions import connection


logger = logging.getLogger("feature-store")


def get_conn() -> connection:
    dsn = "postgresql://loadgen@postgres:5432/default?sslmode=disable"
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


@dataclass
class Lead:
    email: str
    utm_medium: str
    utm_source: str
    id: Optional[int] = None


@dataclass
class ConversionEvent:
    converted_at: float
    lead: Lead
    conversion_amount: int

    def __lt__(self, other: "ConversionEvent") -> bool:
        """This method is required to use this class in a PriorityQueue"""
        return self.converted_at < other.converted_at


def rand_lead() -> Lead:
    """
    Generate a random lead.
    """
    email = "".join(
        random.choice(string.ascii_letters) for _ in range(random.randint(4, 20))
    )
    email += (
        "@"
        + "".join(
            random.choice(string.ascii_letters) for _ in range(random.randint(3, 5))
        )
        + ".com"
    )
    utm_medium = random.choice(("email", "social", "organic", "referral"))
    medium_to_sources = {
        "email": ["klaviyo.com"],
        "social": ["facebook.com", "twitter.com", "instagram.com"],
        "organic": ["none", "google.com"],
        "referral": ["hackernews.com", "reddit.com"],
    }
    utm_source = random.choice(medium_to_sources[utm_medium])
    return Lead(email=email, utm_medium=utm_medium, utm_source=utm_source)


def log_conversion(conn: connection, lead: Lead, conversion_amount: int) -> None:
    """
    Log a conversion to the database.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE 
              leads 
            SET 
              converted_at = NOW()
              , conversion_amount = {conversion_amount}
            WHERE id = {lead.id};
            """
        )


def log_coupon(conn: connection, lead: Lead, amount: int) -> None:
    """
    Log a coupon to the database
    """
    logger.debug(f"Logging coupon of amount {amount} to lead {lead.id}.")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO coupons (lead_id, amount)
                VALUES ({lead.id}, {amount});
            """
        )


def convert(queue: PriorityQueue) -> None:
    """
    Convert a lead.

    This function consumes leads to convert from a queue. The lead is converted by
    marking them as converted in the database.
    """
    conn = get_conn()
    PERIOD = 0.05
    while True:
        conversion_event = queue.get()
        time_to_wait = conversion_event.converted_at - time.time()
        if time_to_wait > PERIOD:
            # Put item back onto the queue
            queue.put(conversion_event)
            time.sleep(PERIOD)
        else:
            logger.debug(
                f"Logging conversion of amount {conversion_event.conversion_amount} for"
                f" lead {conversion_event.lead.id}. Difference between now and "
                f"converted_at = {time.time() - conversion_event.converted_at}"
            )
            log_conversion(
                conn, conversion_event.lead, conversion_event.conversion_amount
            )


def log_prediction(
    lead: Lead,
    score: float,
    label: int,
    producer: KafkaProducer,
    experiment_bucket: str,
) -> None:
    """
    Log a conversion prediction to the RedPanda queue.
    """
    record = {
        "lead_id": lead.id,
        "experiment_bucket": experiment_bucket,
        "score": score,
        "label": label,
        "predicted_at": dt.datetime.now(tz=dt.timezone.utc).isoformat(),
    }
    logger.debug(f"Logging prediction {record}")
    val = json.dumps(record, ensure_ascii=False).encode("utf-8")
    producer.send(topic="conversion_predictions", value=val)


def create_lead(
    conn: connection,
    queue: PriorityQueue,
    producer: KafkaProducer,
) -> None:
    """
    Create a lead and fire a conversion prediction. If the prediction score is below
    0.5, then we create a randomly-valued coupon for the user. We then flip a coin to
    decide if the user actually converts. If they do convert, then we pick a random
    conversion amount and add an event to the queue to have them convert at some random
    time in the future.

    There's a lot of randomness.
    """
    lead = rand_lead()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO leads (email, utm_medium, utm_source)
              VALUES ('{lead.email}', '{lead.utm_medium}', '{lead.utm_source}')
              RETURNING id;
            """
        )
        res = cur.fetchone()
        if res is None:
            raise RuntimeError(f"Failed to create lead {lead}")
        else:
            lead.id = res[0]

        # Sacrifice to the mypy gods.
        assert lead.id is not None

        logger.debug(f"Created lead {lead}")

        # Make a "prediction"
        score = random.random()
        threshold = 0.5
        label = int(score > threshold)

        # Reserve 20% of leads to a control group where we don't offer them the coupon.
        experiment_bucket = "control" if lead.id % 10 <= 1 else "experiment"
        log_prediction(lead, score, label, producer, experiment_bucket)

        # If we don't think they're going to convert and they're not in the control
        # group, then send a coupon.
        sent_coupon = False
        if not label and experiment_bucket != "control":
            # If we don't think they're going to convert, then let's try a coupon.
            # Coupon amount is randomized between $5 and $50
            coupon_amount = random.randint(500, 5_000)
            log_coupon(conn, lead, coupon_amount)
            sent_coupon = True

    # Decide whether or not they converted. Let's assume that the predicted conversion
    # probability is well correlated with converting.
    # The below calculation basically ensures that X% of leads with a score of 0.X will
    # convert.
    did_convert = random.random() < score

    # We'll also assume that the coupon makes the lead a little more likely to convert.
    # We'll give them another chance if they didn't convert in the previous calculation.
    if sent_coupon and not did_convert:
        did_convert = random.random() < score

    if did_convert:
        # We assume that they convert over some time in the next 30 seconds.
        converted_at = time.time() + random.random() * 30
        logger.debug(f"Pushing conversion onto the queue.")
        # Pick a random conversion amount in cents between $10 and $250.
        conversion_amount = random.randint(1_000, 25_000)
        # Add this conversion to the queue so that it gets consumed and the lead
        # converts in the future.
        queue.put(ConversionEvent(converted_at, lead, conversion_amount))


def run_simulation() -> None:
    """
    Simulate a SaaS product where

    1. Users land on a home page and become leads.
    2. Lead info gets logged to the database.
    3. Predict the probability the lead will convert and log that prediction to
      RedPanda.
    4. If the lead is unlikely to convert, then create a coupon for them.
    5. Randomly decide which leads convert, and have them convert during some random
      time in the next 30 seconds.

    Leads are created roughly every 10 milliseconds. In order to have the lead convert
    at some time in the future, we create a priority queue to consume these conversion
    "events" and run them at the specified conversion time.
    """
    KAFKA_BROKER = "redpanda:9092"
    producer = KafkaProducer(bootstrap_servers=[KAFKA_BROKER])
    queue: PriorityQueue[ConversionEvent] = PriorityQueue()
    thread = threading.Thread(target=convert, args=(queue,))
    thread.start()

    conn = get_conn()
    logger.info("Starting loadgen.")
    while True:
        create_lead(conn, queue, producer)
        time.sleep(0.01)


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s %(message)s")
    logger.setLevel(logging.INFO)
    logger.info("Beginning loadgen simulation.")
    run_simulation()
