import { test } from 'node:test';
import assert from 'node:assert/strict';
import {
  CHUNK_SIZE,
  MAX_FILE_SIZE,
  packChunk,
  unpackChunk,
  encodeMeta,
  encodeResume,
  encodeResumeAck,
  encodeEnd,
  encodeReceipt,
  parseControl,
  sha256,
  digestChunkPrefix,
  createTransferId,
} from '../../src/protocol.js';

test('常量符合需求：16 KiB 块、5 MiB 上限', () => {
  assert.equal(CHUNK_SIZE, 16 * 1024);
  assert.equal(MAX_FILE_SIZE, 5 * 1024 * 1024);
});

test('packChunk/unpackChunk 使用 4 字节大端序号并保留载荷', () => {
  const payload = new Uint8Array(CHUNK_SIZE).fill(7);
  const buf = packChunk(0x01020304, payload);
  assert.equal(buf.byteLength, 4 + CHUNK_SIZE);
  assert.equal(new Uint8Array(buf)[0], 0x01);
  const parsed = unpackChunk(buf);
  assert.equal(parsed.sequence, 0x01020304);
  assert.equal(parsed.payload.byteLength, CHUNK_SIZE);
  assert.ok(parsed.payload.every((b) => b === 7));
});

test('unpackChunk 拒绝头部不足与空载荷（坏块）', () => {
  assert.equal(unpackChunk(new ArrayBuffer(3)), null);
  assert.equal(unpackChunk(new ArrayBuffer(4)), null);
  assert.equal(unpackChunk(null), null);
});

test('packChunk 拒绝超过 16 KiB 的块', () => {
  assert.throws(() => packChunk(0, new Uint8Array(CHUNK_SIZE + 1)), /16 ?KiB|16384/);
});

test('meta 往返且 chunks 必须与 size/CHUNK_SIZE 自洽', () => {
  const size = CHUNK_SIZE + 1;
  const HASH = 'a'.repeat(64);
  const text = encodeMeta({ id: 'x', name: 'a.bin', size, chunks: 2, hash: HASH });
  const msg = parseControl(text);
  assert.deepEqual(msg, { kind: 'meta', id: 'x', name: 'a.bin', size, chunks: 2, hash: HASH });
});

test('resume 与 resume-ack 往返，身份/边界/前缀摘要必须自洽', () => {
  const HASH = 'b'.repeat(64);
  const PREFIX = 'c'.repeat(64);
  const text = encodeResume({
    id: 'x', name: 'a.bin', size: CHUNK_SIZE * 2, chunks: 2,
    hash: HASH, nextChunk: 1, prefixHash: PREFIX,
  });
  assert.deepEqual(parseControl(text), {
    kind: 'resume', id: 'x', name: 'a.bin', size: CHUNK_SIZE * 2, chunks: 2,
    hash: HASH, nextChunk: 1, prefixHash: PREFIX,
  });

  assert.deepEqual(parseControl(encodeResumeAck({
    id: 'x', ok: true, nextChunk: 1, prefixHash: PREFIX,
  })), {
    kind: 'resume-ack', id: 'x', ok: true, nextChunk: 1, prefixHash: PREFIX, reason: '',
  });

  assert.equal(parseControl(JSON.stringify({
    kind: 'resume', id: 'x', name: 'n', size: 2, chunks: 1,
    hash: HASH, nextChunk: 2, prefixHash: PREFIX,
  })), null);
  assert.equal(parseControl(JSON.stringify({
    kind: 'resume', id: 'x', name: 'n', size: 2, chunks: 1,
    hash: HASH, nextChunk: 0, prefixHash: '',
  })), null);
  assert.equal(parseControl(JSON.stringify({ kind: 'resume-ack', id: 'x', ok: true })), null);
});

test('parseControl 拒绝各类畸形控制消息', () => {
  assert.equal(parseControl('not json'), null);
  assert.equal(parseControl(JSON.stringify({})), null);
  assert.equal(parseControl(JSON.stringify({ kind: 'meta', id: 'x' })), null);
  assert.equal(
    parseControl(
      JSON.stringify({ kind: 'meta', id: 'x', name: 'n', size: 10, chunks: 99, hash: 'h' }),
    ),
    null,
  );
  assert.equal(
    parseControl(
      JSON.stringify({ kind: 'meta', id: 'x', name: 'n', size: -1, chunks: 0, hash: 'h' }),
    ),
    null,
  );
  assert.equal(
    parseControl(
      JSON.stringify({ kind: 'meta', id: 'x', name: 'n', size: MAX_FILE_SIZE + 1, chunks: 1, hash: 'h' }),
    ),
    null,
  );
  assert.equal(
    parseControl(
      JSON.stringify({ kind: 'meta', id: 'x', name: 'n', size: 10, chunks: 1, hash: 'not-hex' }),
    ),
    null,
  );
  assert.equal(parseControl(JSON.stringify({ kind: 'end', id: 'x' })), null);
  assert.equal(parseControl(JSON.stringify({ kind: 'unknown', id: 'x' })), null);
});

test('end 往返', () => {
  const hash = 'd'.repeat(64);
  assert.deepEqual(parseControl(encodeEnd({ id: 'abc', hash })), {
    kind: 'end',
    id: 'abc',
    hash,
  });
});

test('receipt 往返：成功必须带 hash，失败必须带 reason', () => {
  const hash = 'e'.repeat(64);
  assert.deepEqual(
    parseControl(encodeReceipt({ id: 'abc', ok: true, hash })),
    { kind: 'receipt', id: 'abc', ok: true, hash, reason: '' },
  );
  assert.deepEqual(
    parseControl(encodeReceipt({ id: 'abc', ok: false, reason: 'SHA-256 不匹配' })),
    { kind: 'receipt', id: 'abc', ok: false, hash: '', reason: 'SHA-256 不匹配' },
  );
  assert.equal(parseControl(JSON.stringify({ kind: 'receipt', id: 'x', ok: true })), null);
  assert.equal(parseControl(JSON.stringify({ kind: 'receipt', id: 'x', ok: false })), null);
  assert.equal(parseControl(JSON.stringify({ kind: 'receipt', id: 'x' })), null);
});

test('sha256 对空数据与已知向量正确', async () => {
  assert.equal(await sha256(new Uint8Array(0)),
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
  assert.equal(await sha256(new TextEncoder().encode('abc')),
    'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
});

test('digestChunkPrefix 只接受边界完整的连续块前缀', async () => {
  const a = new Uint8Array([1, 2]);
  const b = new Uint8Array([3, 4]);
  assert.equal(await digestChunkPrefix([a, b], 2), await sha256(new Uint8Array([1, 2, 3, 4])));
  assert.equal(await digestChunkPrefix([a], 1), await sha256(a));
  assert.equal(await digestChunkPrefix([], 0), await sha256(new Uint8Array(0)));
  await assert.rejects(() => digestChunkPrefix([a, undefined, b], 3), /不连续/);
});

test('transfer id 唯一', () => {
  const ids = new Set(Array.from({ length: 100 }, () => createTransferId()));
  assert.equal(ids.size, 100);
});
