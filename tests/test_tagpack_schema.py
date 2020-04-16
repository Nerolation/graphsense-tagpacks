import pytest

from tagpack.tagpack_schema import TagPackSchema, ValidationError
from tagpack.tagpack import TagPack

TEST_SCHEMA = 'tests/testfiles/schema_1.yaml'


@pytest.fixture
def schema(monkeypatch):
    monkeypatch.setattr('tagpack.tagpack_schema.TAGPACK_SCHEMA_FILE',
                        TEST_SCHEMA)
    return TagPackSchema()


def test_init(schema):
    assert isinstance(schema, TagPackSchema)
    assert schema.definition == TEST_SCHEMA


def test_header_fields(schema):
    assert isinstance(schema.header_fields, dict)
    assert 'title' in schema.header_fields
    assert 'type' in schema.header_fields['title']
    assert 'text' in schema.header_fields['title']['type']
    assert 'mandatory' in schema.header_fields['title']
    assert schema.header_fields['title']['mandatory'] is True


def test_mandatory_header_fields(schema):
    assert isinstance(schema.mandatory_header_fields, dict)
    assert 'title' in schema.mandatory_header_fields
    assert 'notmandatory' not in schema.mandatory_header_fields


def test_tag_fields(schema):
    assert isinstance(schema.tag_fields, dict)
    assert 'address' in schema.tag_fields
    assert 'type' in schema.tag_fields['address']
    assert 'mandatory' in schema.tag_fields['address']


def test_mandatory_tag_fields(schema):
    assert isinstance(schema.mandatory_tag_fields, dict)
    assert 'address' in schema.mandatory_tag_fields
    assert 'lastmod' not in schema.mandatory_tag_fields


def test_all_fields(schema):
    assert isinstance(schema.all_fields, dict)
    assert all(field in schema.all_fields
               for field in ['title', 'notmandatory',
                             'address', 'lastmod', 'category'])


def test_field_type(schema):
    assert schema.field_type('title') == 'text'
    assert schema.field_type('lastmod') == 'datetime'


def test_field_taxonomy(schema):
    assert schema.field_taxonomy('category') == 'entity'


def test_validate(schema):
    tagpack = TagPack('http://example.com',
                      'tests/testfiles/tagpack_ok.yaml', schema)
    schema.validate(tagpack, None)


def test_validate_undefined_field(schema):
    tagpack = TagPack('http://example.com',
                      'tests/testfiles/tagpack_fail_undefined_field.yaml',
                      schema)
    with pytest.raises(ValidationError) as e:
        schema.validate(tagpack, None)
    assert "Field failfield not allowed in header" in str(e.value)


def test_validate_fail_type_text(schema):
    tagpack = TagPack('http://example.com',
                      'tests/testfiles/tagpack_fail_type_text.yaml', schema)
    with pytest.raises(ValidationError) as e:
        schema.validate(tagpack, None)
    assert "Field title must be of type text" in str(e.value)


def test_validate_fail_type_date(schema):
    tagpack = TagPack('http://example.com',
                      'tests/testfiles/tagpack_fail_type_date.yaml', schema)
    with pytest.raises(ValidationError) as e:
        schema.validate(tagpack, None)
    assert "Field lastmod must be of type datetime" in str(e.value)


def test_validate_ok_type_datetime(schema):
    tagpack = TagPack('http://example.com',
                      'tests/testfiles/tagpack_ok_type_datetime.yaml', schema)
    schema.validate(tagpack, None)


def test_validate_fail_missing(schema):
    tagpack = TagPack('http://example.com',
                      'tests/testfiles/tagpack_fail_missing.yaml', schema)
    with pytest.raises(ValidationError) as e:
        schema.validate(tagpack, None)
    assert "Mandatory field creator missing" in str(e.value)
