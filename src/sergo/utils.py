from importlib import import_module


def import_string(dotted_path):
    try:
        module_path, class_name = dotted_path.rsplit('.', 1)
    except ValueError as err:
        raise ImportError("%s doesn't look like a module path" % dotted_path) from err

    try:
        module = import_module(module_path)
    except ImportError as err:
        raise ImportError('Error importing module %s: "%s"' % (module_path, err)) from err

    try:
        return getattr(module, class_name)
    except AttributeError:
        raise ImportError('Module "%s" does not define a "%s" attribute/class' % (module_path, class_name))


class LazyImport:
    def __init__(self, module_name):
        self.module_name = module_name
        self.module = None

    def __getattr__(self, name):
        if self.module is None:
            self.module = import_module(self.module_name)
        return getattr(self.module, name)
