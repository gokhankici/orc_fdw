orcfdw
======

Reads ORC formatted files. 'testFileReader' program in the 'out' folder can be used to read such files.

## Installation

1. Clone this repo with the command `git clone https://github.com/gokhankici/orcfdw.git`
2. Install `protobuf-c` library. First [protobuf](https://code.google.com/p/protobuf/) then [protobuf-c](https://code.google.com/p/protobuf-c/) library should be installed. Both can be installed simply by downloading and extracting them and then issuing the following commands in their root folder:

    ```
    .configure
    make
    sudo make install
    ```
2. Run `sh init.sh` and then `make` in orcfdw folder.

## Usage

Executables will be created in the out folder. You can use `testFileReader` program in that folder to read your ORC file or you can try it with one of the files in the 'sample-files' folder.
