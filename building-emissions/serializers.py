import io

from typing import Any, Callable

from prefect.engine.serializers import Serializer


class GeoPandasSerializer(Serializer):
    """A Serializer for GeoPandas GeoDataFrames.
    Args:
        - file_type (str): The type you want the resulting file to be
            saved as, e.g. "csv" or "parquet". Must match a type used
            in a `GeoDataFrame.to_` method and a `gpd.read_` function.
        - deserialize_kwargs (dict, optional): Keyword arguments to pass to the
            serialization method.
        - serialize_kwargs (dict, optional): Keyword arguments to pass to the
            deserialization method.
    """

    def __init__(
        self,
        file_type: str,
        deserialize_kwargs: dict = None,
        serialize_kwargs: dict = None,
    ) -> None:
        self.file_type = file_type

        # Fails fast if user specifies a format that geopandas can't deal with.
        self._get_deserialize_method()
        self._get_serialize_method()

        self.deserialize_kwargs = (
            {} if deserialize_kwargs is None else deserialize_kwargs
        )
        self.serialize_kwargs = {} if serialize_kwargs is None else serialize_kwargs

    def serialize(self, value: "gpd.GeoDataFrame") -> bytes:  # noqa: F821
        """
        Serialize a geopandas GeoDataFrame to bytes.
        Args:
            - value (GeoDataFrame): the GeoDataFrame to serialize
        Returns:
            - bytes: the serialized value
        """
        serialization_method = self._get_serialize_method(GeoDataFrame=value)
        buffer = io.BytesIO()
        try:
            serialization_method(buffer, **self.serialize_kwargs)
            return buffer.getvalue()
        except TypeError:
            # there are some weird bugs with several of the Pandas serialization
            # methods when trying to serialize to bytes directly. This is a
            # workaround. See https://github.com/pandas-dev/pandas/pull/35129
            string_buffer = io.StringIO()
            serialization_method(string_buffer, **self.serialize_kwargs)
            return string_buffer.getvalue().encode()

    def deserialize(self, value: bytes) -> "gpd.GeoDataFrame":  # noqa: F821
        """
        Deserialize an object to a geopandas GeoDataFrame
        Args:
            - value (bytes): the value to deserialize
        Returns:
            - GeoDataFrame: the deserialized GeoDataFrame
        """
        deserialization_method = self._get_deserialize_method()
        buffer = io.BytesIO(value)
        deserialized_data = deserialization_method(buffer, **self.deserialize_kwargs)
        return deserialized_data

    def __eq__(self, other: Any) -> bool:
        if type(self) == type(other):
            return (
                self.file_type == other.file_type
                and self.serialize_kwargs == other.serialize_kwargs
                and self.deserialize_kwargs == other.deserialize_kwargs
            )
        return False

    # _get_read_method and _get_write_method are constructed as they are both to
    # limit copy/paste but also to make it easier for potential future extension to serialization
    # methods that do not map to the "to_{}/read_{}" interface.
    def _get_deserialize_method(self) -> Callable:
        import geopandas as gpd

        try:
            return getattr(gpd, "read_{}".format(self.file_type))
        except AttributeError as exc:
            raise ValueError(
                "Could not find deserialization methods for {}".format(self.file_type)
            ) from exc

    def _get_serialize_method(
        self, GeoDataFrame: "gpd.GeoDataFrame" = None
    ) -> Callable:
        import geopandas as gpd

        if GeoDataFrame is None:
            # If you just want to test if the method exists, create an empty GeoDataFrame
            GeoDataFrame = gpd.GeoDataFrame()
        try:
            return getattr(GeoDataFrame, "to_{}".format(self.file_type))
        except AttributeError as exc:
            raise ValueError(
                "Could not find serialization methods for {}".format(self.file_type)
            ) from exc
