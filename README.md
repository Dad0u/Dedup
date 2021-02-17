# Dedup
File and media deduplication software in Python

## This project is not functionnal yet

The goal of this project is to scan a folder for identical files

It can detect exactly identical files, but also similar images and video

It creates a database of the files and when asked to, it generates a signature from the media.
These signatures are much smaller than the image or video and quicker to compare.

Different approaches are being explored for videos: comparing fragments, using several layers of comparison to quickly filter the matching videos etc...

The goal is to be scalable to large media libraries (100k files) on a home computer
(to accelerate signature generation in large librairies, the use of more than one PC is planned).
