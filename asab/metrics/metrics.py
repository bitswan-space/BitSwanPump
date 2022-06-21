import abc
import copy
import time
from .. import Config


class Metric(abc.ABC):

	def __init__(self, init_values=None):
		self.Init = init_values
		self.Storage = None
		self.StaticTags = dict()
		self.Expiration = float(Config.get("asab:metrics", "expiration"))

	def _initialize_storage(self, storage: dict):
		storage.update({
			'type': self.__class__.__name__,
		})
		self.Storage = storage

		if self.Init is not None:
			self.add_field(self.StaticTags.copy())


	def add_field(self, tags):
		raise NotImplementedError(":-(")


	def locate_field(self, tags):
		fieldset = self.Storage['fieldset']

		if tags is None:
			if len(fieldset) == 1:
				# This is the most typical flow
				return fieldset[0]

			tags = self.StaticTags
		else:
			tags = tags.copy()
			tags.update(self.StaticTags)

		# Seek for field in the fieldset using tags
		for field in self.Storage['fieldset']:
			if field['tags'] == tags:
				return field

		# Field not found, create a new one
		field = self.add_field(tags)

		return field


	def flush(self, now):
		pass


class Gauge(Metric):

	def add_field(self, tags):
		field = {
			"tags": tags,
			"values": self.Init.copy() if self.Init is not None else dict(),
			"expires_at": self.App.time() + self.Expiration,
		}
		self.Storage['fieldset'].append(field)
		return field


	def set(self, name: str, value, tags=None):
		field = self.locate_field(tags)
		field['values'][name] = value
		field["expires_at"] = self.App.time() + self.Expiration

	def flush(self, now):
		self.Storage["fieldset"] = [field for field in self.Storage["fieldset"] if field["expires_at"] >= self.App.time()]


class Counter(Metric):

	def add_field(self, tags):
		field = {
			"tags": tags,
			"values": self.Init.copy() if self.Init is not None else dict(),
			"actuals": self.Init.copy() if self.Init is not None else dict(),
			"expires_at": self.App.time() + self.Expiration,
		}
		self.Storage['fieldset'].append(field)
		return field


	def add(self, name, value, tags=None):
		"""
		:param name: name of the counter
		:param value: value to be added to the counter
		:param init_value: init value, when the counter `name` is not yet set up (f. e. by init_values in the constructor)

		Adds to the counter specified by `name` the `value`.
		If name is not in Counter Values, it will be added to Values.

		"""

		field = self.locate_field(tags)
		actuals = field['actuals']
		try:
			actuals[name] += value
		except KeyError:
			actuals[name] = value
		field["expires_at"] = self.App.time() + self.Expiration


	def sub(self, name, value, tags=None):
		"""
		:param name: name of the counter
		:param value: value to be subtracted from the counter
		:param init_value: init value, when the counter `name` is not yet set up (f. e. by init_values in the constructor)

		Subtracts to the counter specified by `name` the `value`.
		If name is not in Counter Values, it will be added to Values.

		"""

		field = self.locate_field(tags)
		actuals = field['actuals']
		try:
			actuals[name] -= value
		except KeyError:
			actuals[name] = -value
		field["expires_at"] = self.App.time() + self.Expiration


	def flush(self, now):
		self.Storage["fieldset"] = [field for field in self.Storage["fieldset"] if field["expires_at"] >= self.App.time()]
		if self.Storage.get("reset") is True:
			for field in self.Storage['fieldset']:
				field['values'] = field['actuals']
				if self.Init is not None:
					field['actuals'] = self.Init.copy()
				else:
					field['actuals'] = dict()
		else:
			for field in self.Storage['fieldset']:
				field['values'] = field['actuals'].copy()



class EPSCounter(Counter):
	"""
	Event per Second Counter
	Divides all values by delta time
	"""

	def __init__(self, init_values=None):
		super().__init__(init_values=init_values)
		self.LastTime = time.time()


	def flush(self, now):
		self.Storage["fieldset"] = [field for field in self.Storage["fieldset"] if field["expires_at"] >= self.App.time()]
		delta = now - self.LastTime
		if delta <= 0.0:
			return

		reset = self.Storage.get("reset")

		for field in self.Storage['fieldset']:
			field['values'] = {
				k: v / delta
				for k, v in field['actuals'].items()
			}

			if reset is True:
				if self.Init is not None:
					field['actuals'] = self.Init.copy()
				else:
					field['actuals'] = dict()

				self.LastTime = now


class DutyCycle(Metric):
	'''
	https://en.wikipedia.org/wiki/Duty_cycle

		now = self.Loop.time()
		d = now - self.LastReadyStateSwitch
		self.LastReadyStateSwitch = now
	'''


	def __init__(self, loop, init_values=None):
		super().__init__()
		self.Loop = loop
		now = self.Loop.time()
		self.EmptyValue = {
			"on_off": None,
			"timestamp": now,
			"off_cycle": 0.0,
			"on_cycle": 0.0
		}

		self.Init = dict()

		if init_values is not None:
			for k, v in init_values.items():
				value = self.EmptyValue.copy()
				value["on_off"] = v
				self.Init[k] = value

	def add_field(self, tags):
		field = {
			"tags": tags,
			"actuals": self.Init.copy(),
			"values": dict(),
			"expires_at": self.App.time() + self.Expiration,
		}
		self.Storage['fieldset'].append(field)
		return field


	def set(self, name, on_off: bool, tags=None):
		field = self.locate_field(tags)
		now = self.Loop.time()
		values = field["actuals"].get(name)
		if values is None:
			value = self.EmptyValue.copy()
			value["on_off"] = on_off
			value["timestamp"] = now
			field["actuals"][name] = value
			return

		if values.get("on_off") == on_off:
			return  # No change

		d = now - values.get("timestamp")
		off_cycle = values.get("off_cycle")
		on_cycle = values.get("on_cycle")
		if on_off:
			# From off to on
			off_cycle += d
		else:
			# From on to off
			on_cycle += d

		field["actuals"][name]["on_off"] = on_off
		field["actuals"][name]["timestamp"] = now
		field["actuals"][name]["off_cycle"] = off_cycle
		field["actuals"][name]["on_cycle"] = on_cycle

		field["expires_at"] = self.App.time() + self.Expiration


	def flush(self, now):
		self.Storage["fieldset"] = [field for field in self.Storage["fieldset"] if field["expires_at"] >= self.App.time()]
		for field in self.Storage["fieldset"]:
			actuals = field.get("actuals")
			for v_name, values in actuals.items():
				d = now - values.get("timestamp")
				off_cycle = values.get("off_cycle")
				on_cycle = values.get("on_cycle")
				if values.get("on_off"):
					on_cycle += d
				else:
					off_cycle += d

			full_cycle = on_cycle + off_cycle
			if full_cycle > 0.0:
				field["values"][v_name] = on_cycle / full_cycle

			new_value = self.EmptyValue.copy()
			new_value["on_off"] = values.get("on_off")
			new_value["timestamp"] = now

			field["actuals"][v_name] = new_value


class AggregationCounter(Counter):
	'''
	Sets value aggregated with the last one.
	Takes a function object as the `aggregator` argument.
	The aggregation function can take two arguments only.
	Maximum is used as a default aggregation function.
	'''
	def __init__(self, init_values=None, aggregator=max):
		super().__init__(init_values=init_values)
		self.Aggregator = aggregator

	def set(self, name, value, tags=None):
		field = self.locate_field(tags)
		actuals = field['actuals']
		try:
			actuals[name] = self.Aggregator(value, actuals[name])
		except KeyError:
			actuals[name] = value

		field["expires_at"] = self.App.time() + self.Expiration

	def add(self, name, value, tags=None):
		raise NotImplementedError("Do not use add() method with AggregationCounter. Use set() instead.")

	def sub(self, name, value, tags=None):
		raise NotImplementedError("Do not use sub() method with AggregationCounter. Use set() instead.")


class Histogram(Metric):
	"""
	Creates cumulative histograms.
	"""
	def __init__(self, buckets: list):
		super().__init__()
		_buckets = [float(b) for b in buckets]

		if _buckets != sorted(buckets):
			raise ValueError("Buckets not in sorted order")

		if _buckets and _buckets[-1] != float("inf"):
			_buckets.append(float("inf"))

		if len(_buckets) < 2:
			raise ValueError("Must have at least two buckets")

		self.InitBuckets = {b: dict() for b in _buckets}
		self.Buckets = copy.deepcopy(self.InitBuckets)
		self.Count = 0
		self.Sum = 0.0
		self.Init = {
			"buckets": self.InitBuckets,
			"sum": 0.0,
			"count": 0
		}

	def add_field(self, tags):
		field = {
			"tags": tags,
			"values": copy.deepcopy(self.Init),
			"actuals": copy.deepcopy(self.Init),
			"expires_at": self.App.time() + self.Expiration,
		}
		self.Storage['fieldset'].append(field)
		return field

	def flush(self, now):
		self.Storage["fieldset"] = [field for field in self.Storage["fieldset"] if field["expires_at"] >= self.App.time()]
		if self.Storage.get("reset") is True:
			for field in self.Storage['fieldset']:
				field['values'] = field['actuals']
				field['actuals'] = copy.deepcopy(self.Init)
		else:
			for field in self.Storage['fieldset']:
				field['values'] = copy.deepcopy(field['actuals'])

	def set(self, value_name, value, tags=None):
		field = self.locate_field(tags)
		buckets = field.get("actuals").get("buckets")
		summary = field.get("actuals").get("sum")
		count = field.get("actuals").get("count")
		for upper_bound in buckets:
			if value <= upper_bound:
				if buckets[upper_bound].get(value_name) is None:
					buckets[upper_bound][value_name] = 1
				else:
					buckets[upper_bound][value_name] += 1
		field.get("actuals")["sum"] = summary + value
		field.get("actuals")["count"] = count + 1

		field["expires_at"] = self.App.time() + self.Expiration
