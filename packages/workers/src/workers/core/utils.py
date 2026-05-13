import re


def to_snake_case(name: str) -> str:
    """
    Convert a CamelCase or PascalCase string to snake_case.

    This implementation uses a two-pass regular expression to handle
    standard PascalCase as well as sequences of capital letters
    (acronyms), ensuring they are separated correctly.

    Parameters
    ----------
    name : str
        The string to be converted. Typically a class name or
        type identifier.

    Returns
    -------
    str
        The converted snake_case string in all lowercase.

    Examples
    --------
    >>> to_snake_case("ProducerWorker")
    'producer_worker'
    >>> to_snake_case("NATSProducer")
    'nats_producer'
    >>> to_snake_case("MyXMLParser")
    'my_xml_parser'
    """
    # First pass: Insert an underscore before any capital letter followed by a lowercase letter.
    # This handles the start of new words.
    # Example: "ProducerWorker" -> "Producer_Worker"
    partial_name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)

    # Second pass: Insert an underscore between a lowercase/number and a capital letter.
    # This handles transitions to acronyms or final words.
    # Example: "myXML" -> "my_XML"
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", partial_name).lower()
