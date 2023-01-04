import pydantic

from pydantic_avro.mixins import AvroMixin


class AvroBase(AvroMixin, pydantic.BaseModel):
    pass
