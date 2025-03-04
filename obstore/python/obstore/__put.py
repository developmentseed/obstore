from typing import Literal, TypeAlias, TypedDict


class UpdateVersion(TypedDict, total=False):
    """Uniquely identifies a version of an object to update.

    Stores will use differing combinations of `e_tag` and `version` to provide
    conditional updates, and it is therefore recommended applications preserve both
    """

    e_tag: str | None
    """The unique identifier for the newly created object.

    <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    """

    version: str | None
    """A version indicator for the newly created object."""


PutMode: TypeAlias = Literal["create", "overwrite"] | UpdateVersion
"""Configure preconditions for the put operation

There are three modes:

- Overwrite: Perform an atomic write operation, overwriting any object present at the
  provided path.
- Create: Perform an atomic write operation, returning
  [`AlreadyExistsError`][obstore.exceptions.AlreadyExistsError] if an object already
  exists at the provided path.
- Update: Perform an atomic write operation if the current version of the object matches
  the provided [`UpdateVersion`][obstore.UpdateVersion], returning
  [`PreconditionError`][obstore.exceptions.PreconditionError] otherwise.

If a string is provided, it must be one of:

- `"overwrite"`
- `"create"`

If a `dict` is provided, it must meet the criteria of
[`UpdateVersion`][obstore.UpdateVersion].
"""


class PutResult(TypedDict):
    """Result for a put request."""

    e_tag: str | None
    """
    The unique identifier for the newly created object

    <https://datatracker.ietf.org/doc/html/rfc9110#name-etag>
    """

    version: str | None
    """A version indicator for the newly created object."""
