#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import List, Sequence

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer


class EmbeddingRequest(BaseModel):
    input: str | List[str]
    model: str | None = None
    dimensions: int | None = Field(default=None, ge=1)
    encoding_format: str | None = None


class EmbeddingDatum(BaseModel):
    object: str = "embedding"
    embedding: List[float]
    index: int


class UsagePayload(BaseModel):
    prompt_tokens: int
    total_tokens: int


class EmbeddingResponse(BaseModel):
    object: str = "list"
    data: List[EmbeddingDatum]
    model: str
    usage: UsagePayload


class HealthResponse(BaseModel):
    status: str
    model: str
    embedding_dimension: int


class ModelCard(BaseModel):
    id: str
    object: str = "model"
    owned_by: str = "local"


class ModelListResponse(BaseModel):
    object: str = "list"
    data: List[ModelCard]


@dataclass
class ServerSettings:
    model_name: str
    embedding_dimension: int | None
    normalize_embeddings: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a local OpenAI-compatible embeddings endpoint backed by sentence-transformers.",
    )
    parser.add_argument(
        "--model",
        default="sentence-transformers/all-MiniLM-L6-v2",
        help="SentenceTransformers model name to load.",
    )
    parser.add_argument(
        "--embedding-dimension",
        type=int,
        default=None,
        help="Optional output dimension. Values smaller than the model width are served via truncate_dim.",
    )
    parser.add_argument(
        "--normalize-embeddings",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Whether to L2-normalize embeddings before returning them.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8085)
    parser.add_argument("--log-level", default="info")
    return parser.parse_args()


def normalize_inputs(payload: str | Sequence[str]) -> List[str]:
    if isinstance(payload, str):
        values = [payload]
    else:
        values = list(payload)

    if not values:
        raise HTTPException(status_code=400, detail="input must not be empty")

    normalized = [str(value) for value in values]
    if any(not value.strip() for value in normalized):
        raise HTTPException(status_code=400, detail="input items must not be blank")
    return normalized


def estimate_tokens(inputs: Sequence[str]) -> int:
    return sum(max(1, len(text.split())) for text in inputs)


def build_app(settings: ServerSettings) -> FastAPI:
    model_kwargs = {}
    if settings.embedding_dimension is not None:
        model_kwargs["truncate_dim"] = settings.embedding_dimension

    model = SentenceTransformer(settings.model_name, **model_kwargs)
    default_dimension = settings.embedding_dimension or model.get_sentence_embedding_dimension()

    app = FastAPI(title="Local Embedding Server", version="0.1.0")
    app.state.model = model
    app.state.settings = settings
    app.state.default_dimension = int(default_dimension)

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            model=settings.model_name,
            embedding_dimension=app.state.default_dimension,
        )

    @app.get("/v1/models", response_model=ModelListResponse)
    async def list_models() -> ModelListResponse:
        return ModelListResponse(data=[ModelCard(id=settings.model_name)])

    @app.post("/v1/embeddings", response_model=EmbeddingResponse)
    async def create_embeddings(request: EmbeddingRequest) -> EmbeddingResponse:
        inputs = normalize_inputs(request.input)
        dimensions = int(request.dimensions or app.state.default_dimension)
        if dimensions <= 0:
            raise HTTPException(status_code=400, detail="dimensions must be positive")
        if dimensions > app.state.default_dimension:
            raise HTTPException(
                status_code=400,
                detail=(
                    "dimensions must not exceed the configured server dimension "
                    f"({app.state.default_dimension})"
                ),
            )

        embeddings = app.state.model.encode(
            inputs,
            convert_to_numpy=True,
            normalize_embeddings=settings.normalize_embeddings,
            show_progress_bar=False,
            truncate_dim=dimensions,
        )

        data = [
            EmbeddingDatum(index=index, embedding=vector.tolist())
            for index, vector in enumerate(embeddings)
        ]
        token_count = estimate_tokens(inputs)
        return EmbeddingResponse(
            data=data,
            model=request.model or settings.model_name,
            usage=UsagePayload(prompt_tokens=token_count, total_tokens=token_count),
        )

    return app


def main() -> None:
    args = parse_args()
    if args.embedding_dimension is not None and args.embedding_dimension <= 0:
        raise SystemExit("--embedding-dimension must be a positive integer")

    settings = ServerSettings(
        model_name=args.model,
        embedding_dimension=args.embedding_dimension,
        normalize_embeddings=args.normalize_embeddings,
    )
    app = build_app(settings)
    uvicorn.run(app, host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()