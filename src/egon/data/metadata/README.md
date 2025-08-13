# egon-data Metadata

The metadata module provides all metadata related functionality used to create/update metadata for all datasets used in the eGon-data pipeline. Functionality defined will be imported into the dataset task.

The metadata module is/offers:

    - the central place to configuring the metadata generation
    - it provides a metadata-template system which applies information like the project context description to all dataset resources by default to ease handling redundant information.
    - Metadata can be setup using YAML files or dict data structures when working in python code using the OMI package
    - A central functionality to upload metadata to the internal database as SQL comment on table <---## We could also use a jsonb column to avoid parsing metadata from string
    - Provides a simple way to fill the metadata structure with content while keep oemetadata specification compliance
    - Validate metadata against the json schema specification
    - Create a single datapackage which lists all metadata from its resources as defined in a dataset dependency list. It represents the dataset which is then either patly or fully published on the OEP.
    - Store generated metadata

The metadata module is used in dataset modules:

    - To add individual metadata elements using the predefined structure offered by the metadata module
    - Add information about the data model derived from the technical schema as implemented in the database reading form sqlalchemy definitions

Additional use cases

    - Well described metadata is used in the process to upload datasets to the OEP
