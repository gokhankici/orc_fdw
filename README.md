orc\_fdw
========

Foreign data wrapper for reading ORC formatted files.

## Installation

1. Clone this repo to the `contrib` directory of postgresql source code with the command `git clone https://github.com/gokhankici/orc_fdw.git`
2. Install `protobuf-c` library. First [protobuf](https://code.google.com/p/protobuf/) then [protobuf-c](https://code.google.com/p/protobuf-c/) library should be installed. Both can be installed simply by downloading and extracting them and then issuing the following commands in their root folder:

    ```
    .configure
    make
    sudo make install
    ```
2. Run `make install` in orc\_fdw folder.
