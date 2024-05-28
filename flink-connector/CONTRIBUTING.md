# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

## Before You Begin

Please review the repo-level
[CONTRIBUTING](https://github.com/GoogleCloudPlatform/pubsub/blob/master/CONTRIBUTING.md)
for legal requirements and our community guidelines. The rest of this page
describes the contribution process for the `flink-connector` project.

## Contribution Process

### Discuss Your Changes (optional)

Before you start working on a contribution, consider getting in touch with us
first by
[creating an issue](https://github.com/GoogleCloudPlatform/pubsub/issues/new)
with the label "flink connector" to discuss your idea. This is especially
important for large, significant, or speculative changes. Starting a discussion
and getting consensus up front can avoid a lot of frustration during code
review.

### Building and Running Tests

Build, package, and run tests.

```sh
mvn clean verify

# Skip integration tests.
mvn clean verify -pl '!flink-connector-gcp-pubsub-e2e-tests'
```

Run integration and unit tests.

```sh
mvn clean test
```

Run only unit tests.

```sh
mvn clean test -pl '!flink-connector-gcp-pubsub-e2e-tests'
```

#### Running Integration Tests

Integration tests connect to a local instance of the
[Cloud Pub/Sub emulator](https://cloud.google.com/pubsub/docs/emulator). By
default, integration tests start a test container that hosts the emulator. The
container is cleaned up after the test finishes.

In addition to connecting to test containers, integration tests can connect to
emulators started outside of the test process. If the environment variable
`PUBSUB_EMULATOR_HOST` is set, integration tests skip starting a test container
and try to connect to the specified endpoint.

Start a Cloud Pub/Sub emulator.

```sh
# The emulator listens on port 8085 by default.
gcloud beta emulators pubsub start
```

Run integration tests against the local emulator.

```sh
PUBSUB_EMULATOR_HOST=localhost:8085 mvn test
```

### Code Reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

These guidelines will help get your contributions merged smoothly and quickly.

-   Create **small PRs** that are narrowly focused on **addressing a single
    concern**. Create more PRs to address different concerns.

-   Features and behavior changes should generally include builder options that
    allow other connector users to disable them. These type of changes should be
    disabled by default, unless there is a compelling reason to enable these by
    default.

-   Provide a good **PR description** as a record of **what** change is being
    made and **why** it was made. Link to a GitHub issue if it exists.

-   If you are adding a new file, make sure it has the copyright message
    template at the top as a comment. You can copy over the message from an
    existing file and update the year.

-   Maintain **clean commit history** and use **meaningful commit messages**.
    PRs with messy commit history are difficult to review. Push changes that
    address PR review feedback as a separate or multiple commits (avoid vague
    messages like `addressed pr feedback`). Commits will be squashed into one
    when merging PRs.
