"""TagPack - A wrapper for TagPacks files"""
import datetime
import json
import os
import sys
import time
import yaml


def fields_to_timestamp(d):
    """Converts all datetime dict entries to int (timestamp)"""
    for k, v in d.items():
        if isinstance(v, datetime.date):
            d[k] = int(time.mktime(v.timetuple()))
    return d


class TagPackFileError(Exception):
    """Class for TagPack file (structure) errors"""

    def __init__(self, message):
        super().__init__(message)


class TagPack(object):
    """Represents a TagPack"""

    def __init__(self, baseuri, filename, schema):
        self.baseuri = baseuri
        self.filename = filename
        self.schema = schema
        self.tagpack = self.load_tagpack_from_file()
        self.check_tag_pack_structure()

    def load_tagpack_from_file(self):
        if not os.path.isfile(self.filename):
            sys.exit("This program requires {} to be a file"
                     .format(self.filename))
        return yaml.safe_load(open(self.filename, 'r'))

    def check_tag_pack_structure(self):
        self.header_fields
        self.generic_tag_fields
        self.tags

    @property
    def tagpack_uri(self):
        return self.baseuri + '/' + self.filename

    @property
    def header_fields(self):
        try:
            return {k: v for k, v in self.tagpack.items()}
        except AttributeError:
            raise TagPackFileError("Cannot extract TagPack fields")

    @property
    def generic_tag_fields(self):
        try:
            return {k: v for k, v in self.tagpack.items()
                    if k != 'tags' and k in self.schema.tag_fields}
        except AttributeError:
            raise TagPackFileError("Cannot extract TagPack fields")

    @property
    def tags(self):
        try:
            return [Tag(tag, self) for tag in self.tagpack['tags']]
        except AttributeError:
            raise TagPackFileError("Cannot extract TagPack fields")

    def __str__(self):
        return str(self.tagpack)


class Tag(object):
    """Represents a single Tag"""

    def __init__(self, tag, tagpack):
        self.tag = tag
        self.tagpack = tagpack

    @property
    def explicit_fields(self):
        """Return only explicitly defined tag fields"""
        return {k: v for k, v in self.tag.items()}

    @property
    def fields(self):
        """Return all tag fields (explicit and generic)"""
        tag = {}
        for k, v in self.explicit_fields.items():
            tag[k] = self.tag[k]
        for k, v in self.tagpack.generic_tag_fields.items():
            tag[k] = self.tagpack.generic_tag_fields[k]
        tag = fields_to_timestamp(tag)
        return tag

    def to_json(self):
        tag = self.fields
        tag['tagpack_uri'] = self.tagpack.tagpack_uri
        return json.dumps(tag)

    def __str__(self):
        return str(self.fields)
