#!/usr/bin/env python3
# Скрипт, который распознает каждую аудиозапись через Tinkoff VoiceKit
# и помещает результаты распознавания в файл .json

import os
import time
import grpc
import json

from tinkoff.cloud.stt.v1 import stt_pb2_grpc, stt_pb2
from auth import authorization_metadata
from tinkoff.cloud.longrunning.v1 import longrunning_pb2_grpc, longrunning_pb2
from tinkoff.cloud.longrunning.v1.longrunning_pb2 import OperationState, FAILED, ENQUEUED, DONE, PROCESSING

endpoint = os.environ.get("VOICEKIT_ENDPOINT") or "api.tinkoff.ai:443"
api_key = os.environ.get("VOICEKIT_API_KEY") or "ZmthdoiWd6FtTycZCQgm0cyb9OvL7zyG6FgHfd9lEUg="
secret_key = os.environ.get("VOICEKIT_SECRET_KEY") or "XiAxDyNagEO45wMttZNf9EpAVAuwLihU91Lirfugx3U="

audio_folder="audio_1ch"
operation_id_link={}

def build_recognize_request(voice_filename):
    request = stt_pb2.LongRunningRecognizeRequest()
    with open(voice_filename, "rb") as f:
        request.audio.content = f.read()
    request.config.sample_rate_hertz = 44100
    request.config.num_channels = 1
    request.config.encoding = stt_pb2.AudioEncoding.MPEG_AUDIO
    request.config.max_alternatives = 1
    request.config.enable_sentiment_analysis = True
    request.config.enable_gender_identification = True
    return request

def build_get_operation_request(id):
    request = longrunning_pb2.GetOperationRequest()
    request.id = id
    return request

def print_longrunning_operation(operation):
    print("File:",operation_id_link[operation.id]['progress'])
    print("Operation id:", operation.id)
    print("State:", OperationState.Name(operation.state))
    if operation.state == DONE:
        response = stt_pb2.RecognizeResponse()
        operation.response.Unpack(response)
        print_recognition_response(response, operation.id)
    if operation.state == FAILED:
        print("Error:", operation.error)
    print("============================")


def print_recognition_response(response, operation_id):
    json_result = {}
    json_data_list = []
    for result in response.results:
        item = {}
        item['phrase'] = result.alternatives[0].transcript
        item['negative_sentiment'] = result.sentiment_analysis_result.negative_prob_audio

        if ( result.gender_identification_result.male_proba > result.gender_identification_result.female_proba ):
            item['gender'] = 'male'
        elif ( result.gender_identification_result.male_proba < result.gender_identification_result.female_proba ):
            item['gender'] = 'female'
        else:
            item['gender'] = False

        json_data_list.append(item)

    json_result['full_transcript'] = " ".join([result.alternatives[0].transcript for result in response.results])
    json_result['detaled'] = json_data_list

    json_filename = os.path.join(audio_folder, operation_id_link[operation_id]['file'] )
    with open( json_filename + '.json', 'w', encoding='utf8') as outfile:
        json.dump(json_result, outfile, ensure_ascii=False)


files = [file for file in os.listdir(audio_folder)]
for file in files:
    file_path = os.path.join(audio_folder, file)
    progress_msg = str(files.index(file)+1) + "/" + str(len(files))
    stt_stub = stt_pb2_grpc.SpeechToTextStub(grpc.secure_channel(endpoint, grpc.ssl_channel_credentials()))
    stt_metadata = authorization_metadata(api_key, secret_key, "tinkoff.cloud.stt")
    operation = stt_stub.LongRunningRecognize(build_recognize_request(file_path), metadata=stt_metadata)
    operation_id_link[operation.id] = { 'progress': progress_msg, 'file': file }
    print_longrunning_operation(operation)
    operations_stub = longrunning_pb2_grpc.OperationsStub(grpc.secure_channel(endpoint, grpc.ssl_channel_credentials()))
    operations_metadata = authorization_metadata(api_key, secret_key, "tinkoff.cloud.longrunning")
    while operation.state != FAILED and operation.state != DONE:
        time.sleep(1)
        operation = operations_stub.GetOperation(build_get_operation_request(operation.id), metadata=operations_metadata)
        print_longrunning_operation(operation)

with open( 'operations.json', 'w') as outfile:
    json.dump(operation_id_link, outfile)
