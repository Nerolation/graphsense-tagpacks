"""TagPack - A wrappers TagPack Schema"""
import os
import sys
import yaml


TAGPACK_SCHEMA_FILE = 'conf/tagpack_schema.yaml'


class TagPackSchema(object):
    """Defines the structure of a TagPack and supports validation"""

    def __init__(self):
        self.load_config()

    def load_config(self):
        if not os.path.isfile(TAGPACK_SCHEMA_FILE):
            sys.exit("This program requires a schema config file in {}"
                     .format(TAGPACK_SCHEMA_FILE))
        self.schema = yaml.safe_load(open(TAGPACK_SCHEMA_FILE, 'r'))

    @property
    def header_fields(self):
        return [field for field in self.schema['header']]

    @property
    def mandatory_header_fields(self):
        return [field for field, properties in self.schema['header'].items()
                if properties['mandatory']]

    @property
    def tag_fields(self):
        return [field for field in self.schema['tag']]

    @property
    def mandatory_tag_fields(self):
        return [field for field, properties in self.schema['tag'].items()
                if properties['mandatory']]

    @property
    def all_fields(self):
        header_tag_fields = dict(list(self.schema['header'].items()) +
                                 list(self.schema['tag'].items()))
        return header_tag_fields

    def field_type(self, field):
        return self.all_fields[field]['type']

    def field_taxonomy(self, field):
        return self.all_fields[field]['taxonomy']

    def validate(self, tagpack, taxonomies):
        print("Validating TagPack {}".format(tagpack.filename))
