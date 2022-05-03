# SaaS Conversion Predictions

This is a demo of a service for monitoring performance metrics associated with a conversion prediction model. This demo and code are "heavily inspired" by the official Materialized [demos](https://github.com/MaterializeInc/demos).

## Background

Let's assume that we are a machine learning practitioner who works for Down To Clown, a Direct To Consumer (DTC) company that sells clown supplies. A new user who lands on our website is called a _lead_. When that user purchases their first product, we say that they _converted_.

We built a conversion probability model to predict the probability that a `lead` will convert. We model this as a binary classification problem, and the outcome that we're predicting is whether or not the lead converted. 

If the conversion probability is below some threshold, then we offer the `lead` a `coupon` to entice them to convert. The code in this example sets up a system to track how well the _conversion prediction_ model is working.

## Getting Started

To run this code, you must have [Docker](https://www.docker.com/) installed and have a DockerHub login.

To run the system, run 

```commandline
docker-compose up -d
```

This will spin up a Postgres Database, a Materialized Database, a RedPanda queue, a python simulation script, and a Metabase instance.

To view a real-time dashboard of the model's performance, you need to create a local Metabase account and connect to Materialized:

1. Go to [http://localhost:3030](http://localhost:3030).
2. Enter your information (you can enter whatever you want).
3. Setup a PostgreSQL database with the following info:

    | Field             | Value        |
    |-------------------|--------------|
    | Display name      | AISpy        |
    | Name              | materialize  |
    | Host              | materialized |
    | Port              | 6875         |
    | Database name     | materialize  |
    | Database username | materialize  |
    | Database password | _(empty)_    |
4. Click Next and "Take me to Metabase".
5. Under OUR DATA, click AISpy -> public -> Classifier Metrics.

This will show you the materialized view of the various classifier metrics. You can now click Visualization at the bottom to build graphs.

If you build a graph and add it to a dashboard, then you can create a real-time dashboard by clicking the clock in the upper right and setting the plot to auto-refresh every 1 minute. You will notice that a `#refresh=60` has been added to the end of the URL. If you copy this URL, open a new tab, and paste the URL, you can change the ending to `#refresh=1`, and the dashboard will update every second.

