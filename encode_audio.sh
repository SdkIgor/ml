#!/usr/bin/env bash
raw_voice_dir=audio_new
mono_voice_dir=audio_1ch

mkdir -p $mono_voice_dir

for entry in "$raw_voice_dir"/*
do
  nvbase="$(basename $entry)"
  ffmpeg -i "$entry" -ac 1 "$mono_voice_dir/$nvbase"
done
