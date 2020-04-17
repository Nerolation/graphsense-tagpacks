import datetime
import pytest

from tagpack.tagpack_schema import TagPackSchema
from tagpack.tagpack import TagPack, Tag, fields_to_timestamp


TEST_SCHEMA = 'tests/testfiles/schema_1.yaml'
TEST_TAGPACK = 'tests/testfiles/tagpack_ok.yaml'


@pytest.fixture
def schema(monkeypatch):
    monkeypatch.setattr('tagpack.tagpack_schema.TAGPACK_SCHEMA_FILE',
                        TEST_SCHEMA)
    return TagPackSchema()


@pytest.fixture
def tagpack(schema):
    return TagPack('http://example.com',
                   'tests/testfiles/tagpack_fail_taxonomy_header.yaml',
                   schema)


def test_init(tagpack):
    assert tagpack.baseuri == 'http://example.com'
    assert tagpack.filename == \
        'tests/testfiles/tagpack_fail_taxonomy_header.yaml'
    assert tagpack.schema.definition == TEST_SCHEMA


def test_tagpack_uri(tagpack):
    assert tagpack.tagpack_uri == \
        'http://example.com/tests/testfiles/tagpack_fail_taxonomy_header.yaml'


def test_header_fields(tagpack):
    assert all(field in tagpack.header_fields
               for field in ['title', 'creator', 'lastmod', 'tags'])


def test_generic_tag_fields(tagpack):
    assert all(field in tagpack.generic_tag_fields
               for field in ['lastmod'])


def test_tags(tagpack):
    assert len(tagpack.tags) == 2


def test_tags_explicit_fields(tagpack):
    for tag in tagpack.tags:
        assert all(field in tag.explicit_fields
                   for field in ['address', 'label'])


def test_tags_fields(tagpack):
    for tag in tagpack.tags:
        assert isinstance(tag, Tag)
        assert all(field in tag.fields
                   for field in ['address', 'label', 'lastmod'])
        assert isinstance(tag.fields['lastmod'], int)


def test_fields_to_timestamp():
    test_dict = {'lastmod': datetime.date(1970, 1, 2)}
    result = fields_to_timestamp(test_dict)
    assert isinstance(result['lastmod'], int)
