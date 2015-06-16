# WOWP

**WOWP** (A **WO**rkflo**W** Framework in **P**ython) is a modern, light-weight framework 
for integrated simulations in science.

## Our goal

Wowp enables flow based programming in Python via actors and workflows. We prefer having a clean, straightforward API
for creating and connecting actors and workflows. The target coding style should be as simple as

```python
# connect two actors
actor2.inports['x'] += actor1.outports['y']
# get the workflow
workflow = actor2.get_workflow()
# run the workflow
workflow(x=1)
```

## Documentation
See http://pythonic.eu/wowp

*Copyright (c) 2015 Jakub Urban, Jan Pipek under The MIT License (MIT)*
