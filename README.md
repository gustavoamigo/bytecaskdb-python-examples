[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://github.com/codespaces/new?repo=gustavoamigo/bytecaskdb-python-examples)

# ByteCaskDB Python Examples

Examples of how to use [ByteCaskDB](https://github.com/gustavoamigo/bytecaskdb) with the Python binding.

> **⚠️ Early Development:** This repo uses the latest unstable version of ByteCaskDB. The API may and will probably change and evolve.

## Getting Started with Codespaces

The Codespace environment will start with everything set up. To test, just run:

```bash
python start_here.py
```

Expected output:

```
get(user:1) = b'alice'
contains_key(user:2) = True
del_(user:3) existed = True
get(user:3) after delete = None

After batch:
  get(user:1) = None
  get(user:10) = b'dave'
  get(user:11) = b'eve'

NoSync write: get(fast_key) = b'fast_value'

is_degraded = False
degraded_reason = ''

Done.
```

> **Forked this repo?** Update the Codespaces badge URL to `https://codespaces.new/YOUR_USERNAME/bytecaskdb-python-examples`

## Using in a Different Environment

The pip package is available directly from the repo:

```bash
pip install --extra-index-url https://gustavoamigo.github.io/bytecaskdb/python/latest/ bytecaskdb
```

> **Note:** This is an early development version.

