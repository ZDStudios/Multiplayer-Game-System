import { DatabaseSync } from 'node:sqlite';
import fs from 'node:fs';
import path from 'node:path';

export function openDb(dir) {
  fs.mkdirSync(dir, { recursive: true });
  const db = new DatabaseSync(path.join(dir, 'mp.db'));
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS games (
      id TEXT PRIMARY KEY, name TEXT NOT NULL, key TEXT UNIQUE NOT NULL,
      enabled INTEGER NOT NULL DEFAULT 1, settings TEXT NOT NULL DEFAULT '{}', created INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS kv (
      game TEXT NOT NULL, scope TEXT NOT NULL, owner TEXT NOT NULL, k TEXT NOT NULL,
      v TEXT NOT NULL, updated INTEGER NOT NULL, PRIMARY KEY (game, scope, owner, k));
    CREATE TABLE IF NOT EXISTS accounts (
      game TEXT NOT NULL, username TEXT NOT NULL, hash TEXT NOT NULL, player TEXT NOT NULL,
      created INTEGER NOT NULL, PRIMARY KEY (game, username));
    CREATE INDEX IF NOT EXISTS accounts_player ON accounts (game, player);
    CREATE TABLE IF NOT EXISTS scores (
      game TEXT NOT NULL, board TEXT NOT NULL, player TEXT NOT NULL, name TEXT,
      score REAL NOT NULL, updated INTEGER NOT NULL, PRIMARY KEY (game, board, player));
    CREATE INDEX IF NOT EXISTS scores_rank ON scores (game, board, score);
    CREATE TABLE IF NOT EXISTS players (
      game TEXT NOT NULL, id TEXT NOT NULL, name TEXT, first_seen INTEGER NOT NULL,
      last_seen INTEGER NOT NULL, PRIMARY KEY (game, id));
  `);

  const q = (sql) => db.prepare(sql);
  const S = {
    metaGet: q('SELECT v FROM meta WHERE k = ?'),
    metaSet: q('INSERT INTO meta (k, v) VALUES (?, ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v'),

    gamesAll: q('SELECT * FROM games ORDER BY created'),
    gameInsert: q('INSERT INTO games (id, name, key, enabled, settings, created) VALUES (?, ?, ?, 1, ?, ?)'),
    gameUpdate: q('UPDATE games SET name = ?, key = ?, enabled = ?, settings = ? WHERE id = ?'),
    gameDelete: q('DELETE FROM games WHERE id = ?'),

    kvGet: q('SELECT v, updated FROM kv WHERE game = ? AND scope = ? AND owner = ? AND k = ?'),
    kvSet: q(`INSERT INTO kv (game, scope, owner, k, v, updated) VALUES (?, ?, ?, ?, ?, ?)
              ON CONFLICT (game, scope, owner, k) DO UPDATE SET v = excluded.v, updated = excluded.updated`),
    kvDel: q('DELETE FROM kv WHERE game = ? AND scope = ? AND owner = ? AND k = ?'),
    kvList: q(`SELECT k, v, updated FROM kv WHERE game = ? AND scope = ? AND owner = ? AND k >= ? AND k < ?
               ORDER BY k LIMIT ?`),
    kvAdminList: q(`SELECT scope, owner, k, length(v) AS size, updated FROM kv WHERE game = ?
                    AND (? = '' OR scope = ?) AND k >= ? AND k < ? ORDER BY updated DESC LIMIT ?`),
    kvCount: q('SELECT COUNT(*) AS n FROM kv WHERE game = ?'),

    accGet: q('SELECT * FROM accounts WHERE game = ? AND username = ?'),
    accByPlayer: q('SELECT username FROM accounts WHERE game = ? AND player = ?'),
    accInsert: q('INSERT INTO accounts (game, username, hash, player, created) VALUES (?, ?, ?, ?, ?)'),
    accCount: q('SELECT COUNT(*) AS n FROM accounts WHERE game = ?'),

    scoreGet: q('SELECT score FROM scores WHERE game = ? AND board = ? AND player = ?'),
    scoreSet: q(`INSERT INTO scores (game, board, player, name, score, updated) VALUES (?, ?, ?, ?, ?, ?)
                 ON CONFLICT (game, board, player) DO UPDATE SET name = excluded.name, score = excluded.score,
                 updated = excluded.updated`),
    scoreTopDesc: q('SELECT player AS id, name, score FROM scores WHERE game = ? AND board = ? ORDER BY score DESC, updated ASC LIMIT ?'),
    scoreTopAsc: q('SELECT player AS id, name, score FROM scores WHERE game = ? AND board = ? ORDER BY score ASC, updated ASC LIMIT ?'),
    scoreRankDesc: q('SELECT COUNT(*) + 1 AS r FROM scores WHERE game = ? AND board = ? AND score > ?'),
    scoreRankAsc: q('SELECT COUNT(*) + 1 AS r FROM scores WHERE game = ? AND board = ? AND score < ?'),
    scoreBoards: q('SELECT board, COUNT(*) AS entries, MAX(updated) AS updated FROM scores WHERE game = ? GROUP BY board'),
    scoreDelBoard: q('DELETE FROM scores WHERE game = ? AND board = ?'),

    playerTouch: q(`INSERT INTO players (game, id, name, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (game, id) DO UPDATE SET name = excluded.name, last_seen = excluded.last_seen`),
    playerGet: q('SELECT * FROM players WHERE game = ? AND id = ?'),
    playerCounts: q('SELECT game, COUNT(*) AS n FROM players GROUP BY game'),
    playersRecent: q('SELECT id, name, first_seen, last_seen FROM players WHERE game = ? ORDER BY last_seen DESC LIMIT ?'),
  };

  const purgeGame = (id) => {
    for (const t of ['kv', 'accounts', 'scores', 'players']) db.prepare(`DELETE FROM ${t} WHERE game = ?`).run(id);
    S.gameDelete.run(id);
  };

  return { db, S, purgeGame, close: () => db.close() };
}
