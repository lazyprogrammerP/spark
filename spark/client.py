import hashlib
import json
import random
from typing import Dict, List, Optional

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from spark.common.types import DataRow, Record
from spark.common.utils import check_sample_rate, get_current_timestamp, is_empty, is_not_empty


class Spark:
    ENDPOINT = "http://collector.newron.ai:4318/v1/traces"

    def __init__(self, application):
        self.application = application

        self.resource = Resource(attributes={SERVICE_NAME: self.application})
        self.exporter = OTLPSpanExporter(endpoint=self.ENDPOINT)

        self.provider = TracerProvider(resource=self.resource)
        self.processor = BatchSpanProcessor(self.exporter)
        self.provider.add_span_processor(self.processor)

        # Set the global trace provider
        trace.set_tracer_provider(self.provider)
        self.tracer = trace.get_tracer(__name__)

    def compute_feedback_id(self, inputs, feedback_keys):
        feedback_id = {k: inputs[k] for k in (feedback_keys or dict(inputs).keys())}
        # Generate a feedback_id based on the input values and feedback keys dictionary
        return hashlib.md5(json.dumps(feedback_id, sort_keys=True).encode("utf-8")).hexdigest()

    def log_prediction_attribute(
        self,
        inputs: Dict[str, DataRow],
        outputs: Dict[str, DataRow],
        feedback_keys: List[str],
        ignore_inputs: List[str],
        feedback_id: str,
        timestamp: int,
        version: Optional[str] = "1",
        application_env: Optional[str] = "dev",
    ):
        if not type(inputs).__name__ == "dict":
            raise Exception("Excpected type of 'inputs' to be 'dict' but instead got " + type(inputs).__name__)

        if is_empty(inputs):
            raise Exception("Tried to log a prediction without 'inputs'")

        if is_empty(outputs):
            raise Exception("Tried to log a prediction without 'outputs'")

        if is_empty(feedback_keys):
            feedback_keys = list(dict(inputs).keys())

        for ignored_input in ignore_inputs:
            if inputs.get(ignored_input):
                inputs.pop(ignored_input)

        if not feedback_id:
            feedback_id = self.compute_feedback_id(inputs=inputs, feedback_keys=feedback_keys)

        if not timestamp:
            timestamp = get_current_timestamp()

        with self.tracer.start_as_current_span("prediction") as predSpan:
            predSpan.set_attributes(
                {
                    "application_id": self.application,
                    "application_name": self.application,
                    "application_env": application_env,
                    "inputs": json.dumps(inputs),
                    "outputs": json.dumps(outputs),
                    "feedback_keys": feedback_keys,
                    "ignore_inputs": ignore_inputs,
                    "feedback_id": feedback_id,
                    "timestamp": timestamp,
                    "version": version,
                    "_type": "prediction",
                }
            )

    def log_feedback_attribute(self, feedback_id: int, feedbacks: Dict[str, DataRow], timestamp: Optional[int]):
        with self.tracer.start_as_current_span("feedback") as feedbackSpan:
            feedbackSpan.set_attributes(
                {
                    "application_id": self.application,
                    "feedback_id": feedback_id,
                    "feedback_version": "1",
                    "feedback_content": json.dumps(feedbacks),
                    "feedback_timestamp": timestamp,
                    "user_metadata": json.dumps({"application": self.application}),
                    "_type": "feedback",
                }
            )

    def log_record(self, record: Record = {}, sample_rate: int = 1.00):
        check_sample_rate(sample_rate=sample_rate)
        if random.random() > sample_rate:
            return

        feedback_keys = record.feedback_keys
        feedback_id = record.feedback_id

        if type(record).__name__ == "Record":
            if is_not_empty(feedback_keys) and record.feedback_id:
                raise Exception("Encountered feedback_keys and feedback_id simultaneously")

            inputs = record.inputs
            outputs = record.outputs
            feedbacks = record.feedbacks

            ignore_inputs = record.ignore_inputs
            for ignored_input in ignore_inputs:
                if inputs.get(ignored_input):
                    inputs.pop(ignored_input)

            input_exists = is_not_empty(inputs)
            output_exists = is_not_empty(outputs)
            pred_exists = input_exists and output_exists
            feedback_exists = is_not_empty(feedbacks)

            if isinstance(feedback_id, str):
                pass
            else:
                if input_exists:
                    feedback_id = self.compute_feedback_id(inputs=inputs, feedback_keys=feedback_keys)
                else:
                    raise Exception("Cannot generate feedback id without 'inputs'")

            timestamp = record.timestamp

            # If predicition and feedback both exist, then build the prediction and feedback attribute and log it
            if pred_exists and feedback_exists:
                self.log_prediction_attribute(
                    inputs=inputs,
                    outputs=outputs,
                    feedback_keys=feedback_keys,
                    ignore_inputs=ignore_inputs,
                    feedback_id=feedback_id,
                    timestamp=timestamp,
                    version=record.version,
                    application_env=record.application_env,
                )
                self.log_feedback_attribute(
                    feedback_id=feedback_id,
                    feedbacks=feedbacks,
                    timestamp=timestamp,
                )

                return feedback_id

            # If prediction exists but feedback doesn't then build the predicition attribute with a feedback_id
            elif pred_exists:
                self.log_prediction_attribute(
                    inputs=inputs,
                    outputs=outputs,
                    feedback_keys=feedback_keys,
                    ignore_inputs=ignore_inputs,
                    feedback_id=feedback_id,
                    timestamp=timestamp,
                    version=record.version,
                    application_env=record.application_env,
                )

                return feedback_id

            # If feedback exists but prediction doesn't then build the feedback attribute with the provided feedback_id
            elif feedback_exists:
                self.log_feedback_attribute(
                    feedback_id=feedback_id,
                    feedbacks=feedbacks,
                    timestamp=timestamp,
                )

            else:
                raise Exception("Tried to log a record without prediction and without feedback")

            self.provider.shutdown()
        else:
            raise Exception("Expected type of 'log' to be 'Record' but instead got " + type(record).__name__)

    def log_records(self, records: List[Record] = [], sample_rate: int = 1.00):
        check_sample_rate(sample_rate=sample_rate)
        if random.random() > sample_rate:
            return

        for record in records:
            feedback_keys = record.feedback_keys
            feedback_id = record.feedback_id

            if type(record).__name__ == "Record":
                if is_not_empty(feedback_keys) and record.feedback_id:
                    raise Exception("Encountered feedback_keys and feedback_id simultaneously")

                inputs = record.inputs
                outputs = record.outputs
                feedbacks = record.feedbacks

                ignore_inputs = record.ignore_inputs
                for ignored_input in ignore_inputs:
                    if inputs.get(ignored_input):
                        inputs.pop(ignored_input)

                input_exists = is_not_empty(inputs)
                output_exists = is_not_empty(outputs)
                pred_exists = input_exists and output_exists
                feedback_exists = is_not_empty(feedbacks)

                if isinstance(feedback_id, str):
                    pass
                else:
                    if input_exists:
                        feedback_id = self.compute_feedback_id(inputs=inputs, feedback_keys=feedback_keys)
                    else:
                        raise Exception("Cannot generate feedback id without 'inputs'")

                timestamp = record.timestamp

                # If predicition and feedback both exist, then build the prediction and feedback attribute and log it
                if pred_exists and feedback_exists:
                    self.log_prediction_attribute(
                        inputs=inputs,
                        outputs=outputs,
                        feedback_keys=feedback_keys,
                        ignore_inputs=ignore_inputs,
                        feedback_id=feedback_id,
                        timestamp=timestamp,
                        version=record.version,
                        application_env=record.application_env,
                    )
                    self.log_feedback_attribute(
                        feedback_id=feedback_id,
                        feedbacks=feedbacks,
                        timestamp=timestamp,
                    )

                # If prediction exists but feedback doesn't then build the predicition attribute with a feedback_id
                elif pred_exists:
                    self.log_prediction_attribute(
                        inputs=inputs,
                        outputs=outputs,
                        feedback_keys=feedback_keys,
                        ignore_inputs=ignore_inputs,
                        feedback_id=feedback_id,
                        timestamp=timestamp,
                        version=record.version,
                        application_env=record.application_env,
                    )

                # If feedback exists but prediction doesn't then build the feedback attribute with the provided feedback_id
                elif feedback_exists:
                    self.log_feedback_attribute(
                        feedback_id=feedback_id,
                        feedbacks=feedbacks,
                        timestamp=timestamp,
                    )

                else:
                    raise Exception("Tried to log a record without prediction and without feedback")
            else:
                raise Exception("Expected type of 'log' to be 'Record' but instead got " + type(record).__name__)


### Example Code ###
"""
from spark import Spark
from spark.common.types import Record

spark = Spark("test-app")

feedback_id = spark.log_record(
    Record(
        # feedback_id="eeb5e6467fc5dac2f6e71ed897849bd9",
        inputs={"file": {"value": "https://newron-model.s3.amazonaws.com/test_id.jpg"}, "type": "url"},
        outputs={
            "image": "https://newron-model.s3.amazonaws.com/test_id.jpg",
            "dob": "73121S",
            "sex": "M",
            "doe": "221024",
            "country": "IND",
            "id_card": "102214633",
            "surname": "RAM",
            "name": "GURMIT SINGH CHANAN",
        },
        feedbacks={"status": {"value": "incorrect", "type": "categorical"}, "comment": {"value": "DOB has an additional S, surname seems inaccurate."}},
        application_env="prod",
    )
)

print(feedback_id)
"""
