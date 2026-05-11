# spark_hive_greenplum

<!-- public-repo-status -->
> Status: Legacy/reference. This repository is kept public for implementation notes, but it is not actively supported. Issues and pull requests are disabled unless support is reopened.

# HiveToGreenplum

HiveToGreenplum is a Spark application that reads a Hive table in ORC format and uploads it to a Greenplum database.

## Prerequisites

- Apache Spark
- Hive
- Greenplum

## Usage

To run the application, use the following command:

```
spark-submit --class HiveToGreenplum <path-to-jar> <hive-table-name> <greenplum-table-name> <greenplum-schema> <greenplum-url> <greenplum-user> <greenplum-password>
```

## License

<!-- commercial-license-policy -->
This project is licensed for non-commercial use under the [PolyForm Noncommercial License 1.0.0](https://polyformproject.org/licenses/noncommercial/1.0.0.txt).
Commercial use, resale, paid distribution, marketplace publication, SaaS hosting, or bundling into a paid product requires separate written permission from the author.
Project names, logos, package identifiers, store listings, screenshots, and other branding assets are not licensed for use in forks or redistributed builds.
