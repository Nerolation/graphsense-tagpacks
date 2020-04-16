"""TagPack - A wrappers TagPack Schema"""
import datetime
import os
import sys
import yaml


TAGPACK_SCHEMA_FILE = 'conf/tagpack_schema.yaml'


class ValidationError(Exception):
    """Class for schema validation errors"""

    def __init__(self, message):
        super().__init__("Schema Validation Error: " + message)


class TagPackSchema(object):
    """Defines the structure of a TagPack and supports validation"""

    def __init__(self):
        self.load_schema()
        self.definition = TAGPACK_SCHEMA_FILE

    def load_schema(self):
        if not os.path.isfile(TAGPACK_SCHEMA_FILE):
            sys.exit("This program requires a schema config file in {}"
                     .format(TAGPACK_SCHEMA_FILE))
        self.schema = yaml.safe_load(open(TAGPACK_SCHEMA_FILE, 'r'))

    @property
    def header_fields(self):
        return {k: v for k, v in self.schema['header'].items()}

    @property
    def mandatory_header_fields(self):
        return {k: v for k, v in self.schema['header'].items()
                if v['mandatory']}

    @property
    def tag_fields(self):
        return {k: v for k, v in self.schema['tag'].items()}

    @property
    def mandatory_tag_fields(self):
        return {k: v for k, v in self.schema['tag'].items()
                if v['mandatory']}

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
        # check if mandatory fields exist
        for schema_field in self.mandatory_header_fields:
            if schema_field not in tagpack.all_fields:
                raise ValidationError("Mandatory field {} missing"
                                      .format(schema_field))
        # check all tagpack fields against definitions
        for field, value in tagpack.all_fields.items():
            # check a field is defined
            if field not in self.all_fields:
                raise ValidationError("Field {} not allowed in header"
                                      .format(field))
            # check whether a field's type matches the defintion
            schema_type = self.field_type(field)
            if schema_type == 'text':
                if not isinstance(value, str):
                    raise ValidationError("Field {} must be of type text"
                                          .format(field))
            elif schema_type == 'datetime':
                if not isinstance(value, datetime.date):
                    raise ValidationError("Field {} must be of type datetime"
                                          .format(field))
            elif schema_type == 'list':
                if not isinstance(value, list):
                    raise ValidationError("Field {} must be of type list")
            else:
                raise ValidationError("Unsupported schema type {}"
                                      .format(schema_type))

