"""Provider-neutral messages/tool calling interface and explicit token usage."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class Usage:
    """Known final usage plus conservatively charged attempts with unknown usage."""

    input_tokens: int = 0
    output_tokens: int = 0
    attempts: int = 1
    unknown_attempts: int = 0


@dataclass(frozen=True)
class Completion:
    """Anthropic-shaped content blocks keep tool conversations provider-independent."""

    content: list[dict[str, Any]] = field(default_factory=list)
    usage: Usage = Usage()
    request_id: str | None = None

    @property
    def text(self) -> str:
        """Combine only text blocks; tool calls are dispatched by the bounded diagnosis loop."""
        return "\n".join(block["text"] for block in self.content if block.get("type") == "text")


class LLMError(RuntimeError):
    """Provider failure without embedding API credentials or unredacted response bodies."""


class LLMClient(ABC):
    """Complete one turn; tools use name/description/input_schema dictionaries."""

    max_attempts: int = 1

    @abstractmethod
    async def complete(
        self, messages: list[dict[str, Any]], tools: list[dict[str, Any]], schema: dict[str, Any]
    ) -> Completion:
        """Return text or tool-use blocks with usage; never execute an LLM-supplied tool here."""

    async def aclose(self) -> None:
        """Providers with owned network resources override this optional lifecycle hook."""
        return None
