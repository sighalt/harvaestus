import code


def drop_into_shell(**additional_vars):
    """Drop into a python shell with `additional_vars` as locals."""
    custom_locals = locals()
    custom_locals.update(additional_vars)

    try:
        import IPython
        IPython.start_ipython(user_ns=custom_locals)
    except ImportError:
        code.interact(local=custom_locals)


def form_string_to_dict(form_string):
    lines = [line for line in form_string.split("\n") if line]
    lines = [line.split(": ", maxsplit=1) for line in lines]

    return {
        line[0].strip(): line[1].strip()
        for line
        in lines
    }
