from django import template
register = template.Library()

@register.filter
def add_class_if_exists(bound_field, css):
    try:
        bound_field.field.widget.attrs['class'] = (bound_field.field.widget.attrs.get('class','') + ' ' + css).strip()
    except Exception:
        pass
    return bound_field
