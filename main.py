#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_mongodb_python import DestinationMongoDB


if __name__ == "__main__":
    DestinationMongoDB().run(sys.argv[1:])
