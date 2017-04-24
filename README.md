# Event Data Reports

Tool to provide daily reports of the Event Data system and data. When it runs, it will generate all the reports, backfilling through time if they're missing.

A number of types of reports are specified. Each type produces one 'report'. A Report is a JSON document that contains the following fields:

 - `date` - the date that this report concerns, in `YYYY-MM-DD` format
 - `type` - the name of the type
 - `generated` - ISO8601 timestamp that the report was generated
 - `machine-data` - data that may be consumed by a machine at a later date, e.g. large list of tweet IDs
 - `human-data` - data that will be sent to a human by email. May be identical to `machine-data` if suitable for human consumption.
 - `warnings` - count of warnings. If this is greater than zero, the subject of the email will indicate this. Actual warning messages should be communicated via the `human-data` parameter.

Individual report types are free to collect their own data, but are supplied with the daily query API snapshot. Reports are generally idempotent.

## Config

 - `EPOCH` - earliest day we care about, in YYYY-MM-DD format.
 - `EMAILS` - comma-separated list of email addresses to send all report summaries to.
 - `WARNING_EMAILS` - comma-separated list of email addresses to send report summaries to only if there are warnings.
 - `EMAIL_FROM` - account to send email from
 - `MAX_DAYS` - optional, maximum number of days to look back. Default is 10.
 - `SMTP_USERNAME`
 - `SMTP_PASSWORD`
 - `SMTP_HOST`
 - `S3_KEY`
 - `S3_SECRET`
 - `REPORT_BUCKET_NAME` - S3 bucket name to store reports
 - `REPORT_REGION_NAME` - S3 region name for report bucket
 - `BUS_BUCKET_NAME` - S3 details for Event Bus
 - `BUS_REGION_NAME`
 - `BUS_S3_KEY`
 - `BUS_S3_SECRET`

The address of the Query API is hard-coded because the report is meant to consume public data.

## To run

One-off daily.

    lein run daily

Start daily schedule and block

    lein run schedule

This will fill in the last `n` days worth of reports if missing (currently depending on harcoded `max-days`) and sends yesterday's emails.

## License

Copyright © 2017 Crossref

Distributed under the The MIT License (MIT).
