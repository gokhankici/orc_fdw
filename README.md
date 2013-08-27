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

## Converting To ORC Format

To convert your plain text files into the ORC format, a sample Java program in the `converter` folder can be used. It's a maven project, so [maven](https://maven.apache.org/) should be installed on your system. Hive v0.12 is needed for the fdw, so the provided hive-exec package should be used to compile the code (it isn't added as a maven dependency since it isn't contained in the repos). Eclipse could be used to add the hive-exec package as an external jar file and compile/run the project.
