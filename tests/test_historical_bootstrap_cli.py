from __future__ import annotations

import argparse
import unittest


def _find_parser_factory():
    """Find the top-level argparse factory used by ``main()``.

    ``schema_inspector.cli`` may name it ``build_parser``, ``_build_parser``,
    or construct it inline. Returns a zero-arg callable that produces the
    parser, or raises if it cannot be found.
    """
    from schema_inspector import cli

    for name in ("build_parser", "_build_parser", "make_parser", "_make_parser"):
        fn = getattr(cli, name, None)
        if callable(fn):
            return fn
    raise RuntimeError(
        "schema_inspector.cli does not expose a build_parser factory. "
        "Refactor main() to extract the argparse.ArgumentParser construction "
        "into a top-level build_parser() function before adding the bootstrap subcommand."
    )


class WorkerHistoricalBootstrapCLITests(unittest.TestCase):
    def test_subcommand_registered_in_parser(self) -> None:
        parser = _find_parser_factory()()
        subparser_actions = [
            action for action in parser._actions
            if action.__class__.__name__ == "_SubParsersAction"
        ]
        self.assertTrue(subparser_actions, "no subparsers registered on cli parser")
        names: set[str] = set()
        for action in subparser_actions:
            names.update(action.choices.keys())
        self.assertIn("worker-historical-bootstrap", names)

    def test_subparser_has_consumer_name_and_block_ms_flags(self) -> None:
        parser = _find_parser_factory()()
        subparser_actions = [
            action for action in parser._actions
            if action.__class__.__name__ == "_SubParsersAction"
        ]
        sub: argparse.ArgumentParser | None = None
        for action in subparser_actions:
            sub = action.choices.get("worker-historical-bootstrap")
            if sub is not None:
                break
        assert sub is not None
        flags = {a.option_strings[0] for a in sub._actions if a.option_strings}
        self.assertIn("--consumer-name", flags)
        self.assertIn("--block-ms", flags)

    def test_command_added_to_historical_commands_set(self) -> None:
        from schema_inspector.cli import _HISTORICAL_COMMANDS

        self.assertIn("worker-historical-bootstrap", _HISTORICAL_COMMANDS)


if __name__ == "__main__":
    unittest.main()
