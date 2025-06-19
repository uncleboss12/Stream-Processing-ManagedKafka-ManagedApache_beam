```bash
gcloud auth list
gcloud config list project
```

## **Task 1. Create project resources**
In Cloud Shell, create variables for your bucket, project, and region.
```bash
PROJECT_ID=$(gcloud config get-value project)
BUCKET_NAME="${PROJECT_ID}-bucket"
TOPIC_ID=my-id
REGION="filled in at lab start"
```
Set your App Engine region.

Note: For regions other than us-central1 and ```europe-west1```, set the AppEngine region variable to be the same as the assigned region. 
If you are assigned ```us-central1```, set the AppEngine region variable to us-central. 
If you are assigned ```europe-west1```, set the AppEngine region variable to ```europe-west```.
You can refer to the App Engine locations for more information.

```bash
AE_REGION=region_to_be_set
```

Create a Cloud Storage bucket owned by this project:

```bash
gsutil mb gs://$BUCKET_NAME
```

Note: Cloud Storage bucket names must be globally unique. Your Qwiklabs Project ID is always unique, so that is used in your bucket name in this lab.
Create a Pub/Sub topic in this project:

```bash 
gcloud pubsub topics create $TOPIC_ID
```

Create an App Engine app for your project:
```bash
gcloud app create --region=$AE_REGION
```

Create a Cloud Scheduler job in this project. The job publishes a message to a Pub/Sub topic at one-minute intervals:

```bash
gcloud scheduler jobs create pubsub publisher-job --schedule="* * * * *" \ 
    --topic=$TOPIC_ID --message-body="Hello!" ```

If prompted to enable the Cloud Scheduler API, press y and enter.
Click Check my progress to verify the objective.
Create Project Resources

Start the job:
```bash
gcloud scheduler jobs run publisher-job
```

Note: If you encounter an error for RESOURCE_EXHAUSTED, attempt to execute the command again.
Use the following commands to clone the quickstart repository and navigate to the sample code directory:

Python

```bash docker run -it -e DEVSHELL_PROJECT_ID=$DEVSHELL_PROJECT_ID python:3.7 /bin/bash 
git clone https://github.com/GoogleCloudPlatform/python-docs-samples.git
cd python-docs-samples/pubsub/streaming-analytics
pip install -U -r requirements.txt  # Install Apache Beam dependencies
```
Note: If you are using the Python option, execute the Python commands individually.

## **Task 2. Review code to stream messages from Pub/Sub to Cloud Storage**
Code sample
Review the following sample code, which uses Dataflow to:

Read Pub/Sub messages.
Window (or group) messages into fixed-size intervals by publish timestamps.
Write the messages in each window to files in Cloud Storage.

Python
```python
import argparse
from datetime import datetime
import logging
import random

from apache_beam import (
    DoFn,
    GroupByKey,
    io,
    ParDo,
    Pipeline,
    PTransform,
    WindowInto,
    WithKeys,
)
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """

    def __init__(self, window_size, num_shards=5):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
        )


class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )


class WriteToGCS(DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value, window=DoFn.WindowParam):
        """Write messages in a batch to Google Cloud Storage."""

        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        shard_id, batch = key_value
        filename = "-".join([self.output_path, window_start, window_end, str(shard_id)])

        with io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for message_body, publish_time in batch:
                f.write(f"{message_body},{publish_time}\n".encode())


def run(input_topic, output_path, window_size=1.0, num_shards=5, pipeline_args=None):
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            # Because `timestamp_attribute` is unspecified in `ReadFromPubSub`, Beam
            # binds the publish time returned by the Pub/Sub server for each message
            # to the element's timestamp parameter, accessible via `DoFn.TimestampParam`.
            # https://beam.apache.org/releases/pydoc/current/apache_beam.io.gcp.pubsub.html#apache_beam.io.gcp.pubsub.ReadFromPubSub
            | "Read from Pub/Sub" >> io.ReadFromPubSub(topic=input_topic)
            | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards)
            | "Write to GCS" >> ParDo(WriteToGCS(output_path))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_topic",
        help="The Cloud Pub/Sub topic to read from."
        '"projects/<project_id>/topics/<topic_id>".',
    )
    parser.add_argument(
        "--window_size",
        type=float,
        default=1.0,
        help="Output file's window size in minutes.",
    )
    parser.add_argument(
        "--output_path",
        help="Path of the output GCS file including the prefix.",
    )
    parser.add_argument(
        "--num_shards",
        type=int,
        default=5,
        help="Number of shards to use when writing windowed elements to GCS.",
    )
    known_args, pipeline_args = parser.parse_known_args()

    run(
        known_args.input_topic,
        known_args.output_path,
        known_args.window_size,
        known_args.num_shards,
        pipeline_args,
    )
  </topic_id></project_id>
```

Note: To explore the sample code further, visit the respective java-docs-samples and python-docs-samples GitHub pages.


### **Task 3. Start the pipeline**
To start the pipeline, run the following command:

```bash
python PubSubToGCS.py \
    --project=project_id \
    --region=region \
    --input_topic=projects/project_id/topics/my-id \
    --output_path=gs://bucket_name/samples/output \
    --runner=DataflowRunner \
    --window_size=2 \
    --num_shards=2 \
    --temp_location=gs://bucket_name/temp
```

Note: When executing the python command, replace project_id, bucket_name, and region with your project id, bucket name, and assigned lab region.
The preceding command runs locally and launches a Dataflow job that runs in the cloud.

Note: You may have to wait around 10 minutes for the code to fully execute and for the pipeline job to appear in the Dataflow console in the next task.
Note: If you receive a warning regarding StaticLoggerBinder, you can safely ignore it and move ahead in the lab.
Click Check my progress to verify the objective.

## **Task 4. Observe job and pipeline progress**

Go to Dataflow console to observe the job's progress.

Click Refresh to see the job and the latest status updates.

Dataflow page displaying the information of the pubsubtogcs 0815172250-75a99ab8 job

Click on the job name to open the job details and review the following:
 - Job structure
 - Job logs
 - Stage metrics
Job page displaying the Job summary information

You may have to wait a few more minutes to see the output files in Cloud Storage.

You can see the output files by navigating to Navigation menu > Cloud Storage, and clicking on your bucket name and then clicking Samples.
Bucket details page displaying the output file information

Alternately, you can exit the application in Cloud Shell using CTRL+C (and for the Python option, type exit),
and then execute the command below to list the files that have been written out to Cloud Storage:

```bash
gsutil ls gs://${BUCKET_NAME}/samples/
```

## **Task 5. Cleanup**
If you have not already, exit the application in Cloud Shell using CTRL+C.
For the Python option, type exit to exit the Python environment.

In Cloud Shell, delete the Cloud Scheduler job:
``bash
gcloud scheduler jobs delete publisher-job
```
If prompted "Do you want to continue", press Y and enter.

In the Dataflow console, stop the job by selecting your job name, and clicking Stop.
When prompted, click Stop Job > Cancel to cancel the pipeline without draining.

In Cloud Shell, delete the topic:
```bash
gcloud pubsub topics delete $TOPIC_ID
```
In Cloud Shell, delete the files created by the pipeline:

```bash
gsutil -m rm -rf "gs://${BUCKET_NAME}/samples/output*"
gsutil -m rm -rf "gs://${BUCKET_NAME}/temp/*"
```
In Cloud Shell, delete the Cloud Storage bucket:

```bash
gsutil rb gs://${BUCKET_NAME}
```
