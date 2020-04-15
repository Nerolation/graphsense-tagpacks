"""TagPack - A wrapper for TagPacks files"""
import json
import os
import sys
import yaml


class TagPack(object):
    """Represents a TagPack"""

    def __init__(self, filename, schema):
        print("Loading TagPack from {}".format(filename))
        self.filename = filename
        self.schema = schema
        self.load_tagpack()
        for tag in self.tags:
            print(tag.to_json())

    def load_tagpack(self):
        if not os.path.isfile(self.filename):
            sys.exit("This program requires {} to be a file"
                     .format(self.filename))
        self.tagpack = yaml.safe_load(open(self.filename, 'r'))

    @property
    def header_fields(self):
        return {k: v for k, v in self.tagpack.items()
                if k != 'tags' and k in self.schema.header_fields}

    @property
    def generic_tag_fields(self):
        return {k: v for k, v in self.tagpack.items()
                if k != 'tags' and k in self.schema.tag_fields}

    @property
    def tags(self):
        return [Tag(tag, self) for tag in self.tagpack['tags']]

    def __str__(self):
        return str(self.tagpack)


class Tag(object):
    """Represents a single Tag"""

    def __init__(self, tag, tagpack):
        self.tag = tag
        self.tagpack = tagpack

    @property
    def fields(self):
        return {k: v for k, v in self.tag.items()}

    def to_json(self, include_generic=True):
        if include_generic:
            all_tag = {}
            for k, v in self.fields.items():
                all_tag[k] = self.tag[k]
            for k, v in self.tagpack.generic_tag_fields.items():
                all_tag[k] = self.tagpack.generic_tag_fields[k]
            return all_tag
        else:
            return json.dumps(self.tag)

    def __str__(self):
        return str(self.tag)
