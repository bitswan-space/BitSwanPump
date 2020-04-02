import functools

from ..abc import Expression
from ..builder import ExpressionBuilder


class AND(Expression):
	"""
	Checks if all expressions are true:

		{
			"class": "AND",
			"items": [<EXPRESSION>, <EXPRESSION>...]
		}
	"""

	def __init__(self, app, expression_class_registry, expression: dict):
		super().__init__(app, expression_class_registry, expression)
		self.Items = []
		for item in expression.get("items", []):
			self.Items.append(ExpressionBuilder.build(app, expression_class_registry, item))

	def __call__(self, context, event, *args, **kwargs):
		return functools.reduce(
			lambda x, y: x(context, event, *args, **kwargs) and y(context, event, *args, **kwargs) if isinstance(x, Expression) else x and y(context, event, *args, **kwargs),
			self.Items
		)