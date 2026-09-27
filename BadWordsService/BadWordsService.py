import os
from contextlib import asynccontextmanager
from typing import List

import redis.asyncio as redis
from fastapi import FastAPI, Request
from pydantic import BaseModel, Field, field_validator


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
WORDS_KEY = os.getenv("WORDS_KEY", "badwords")
MAX_WORDS = int(os.getenv("MAX_WORDS", "10000"))
MAX_WORD_LEN = int(os.getenv("MAX_WORD_LEN", "256"))


DEFAULT_WORDS = {"badword", "badword0", "badword1", "badword2"}
SEED_ON_EMPTY = os.getenv("SEED_ON_EMPTY", "true").lower() == "true"

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = redis.from_url(REDIS_URL, decode_responses=True)
    await app.state.redis.ping()

    if SEED_ON_EMPTY and not await app.state.redis.exists(WORDS_KEY):
        # SETNX-семантика: гонка между воркерами не задублирует данные
        async with app.state.redis.pipeline(transaction=True) as pipe:
            pipe.delete(WORDS_KEY)
            pipe.rpush(WORDS_KEY, *DEFAULT_WORDS)
            await pipe.execute()

    yield
    await app.state.redis.aclose()



app = FastAPI(lifespan=lifespan)


class WordsPayload(BaseModel):
    words: List[str] = Field(..., max_length=MAX_WORDS)

    @field_validator("words")
    @classmethod
    def validate_words(cls, v: List[str]) -> List[str]:
        cleaned: List[str] = []
        seen: set[str] = set()
        for i, w in enumerate(v):
            if not isinstance(w, str):
                raise ValueError(f"item {i} is not a string")
            w = w.strip()
            if not w:
                raise ValueError(f"item {i} is empty after strip")
            if len(w) > MAX_WORD_LEN:
                raise ValueError(f"item {i} exceeds {MAX_WORD_LEN} chars")
            if w in seen:
                raise ValueError(f"duplicate word: {w!r}")
            seen.add(w)
            cleaned.append(w)
        return cleaned


@app.get("/get_words", response_model=List[str])
async def get_words(request: Request) -> List[str]:
    return await request.app.state.redis.lrange(WORDS_KEY, 0, -1)


@app.post("/update_words", response_model=List[str])
async def update_words(payload: WordsPayload, request: Request) -> List[str]:
    r = request.app.state.redis
    async with r.pipeline(transaction=True) as pipe:
        pipe.delete(WORDS_KEY)
        if payload.words:
            pipe.rpush(WORDS_KEY, *payload.words)
        await pipe.execute()
    return payload.words
