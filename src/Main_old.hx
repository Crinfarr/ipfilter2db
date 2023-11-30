import sqlite.SqliteError;
import haxe.io.Eof;
import sys.thread.ElasticThreadPool;
import sys.thread.Thread;
import sys.thread.Mutex;
import sys.thread.Deque;
import sqlite.Database;
import sys.io.File;

using StringTools;

class Main_old {
	static function main() {
		final maxlines = 20_140_489;
		final f = File.read('/home/crinfarr/Documents/internet_map/ips.list');
		final queue = new Deque<Array<String>>();
		final mutex = new Mutex();
		final pool = new ElasticThreadPool(24, 5);
		final start = Sys.time();

		var queuedChunks = 0;

		var inserted = 0;
		var delta_inserted = 0;

        var loaded = 0;
        var delta_loaded = 0;

		final dbc = new Database('hosts.db', ReadWrite);
		dbc.open();
		dbc.exec("
        CREATE TABLE IF NOT EXISTS hosts (
            ip TEXT NOT NULL,
            --sqlite has no static length varchar
            port INTEGER NOT NULL,
            ts INTEGER NOT NULL
        );")
			.then((_) -> {
				trace("DB Created");
				return;
			}, (e:SqliteError) -> {
				trace('DB Creation error', e.name, e.message);
				return;
			});
		Thread.create(() -> {
			while (!f.eof()) {
				if (queuedChunks >= 10_000) {
					Sys.sleep(10);
					continue;
				}
				final chunkLen = 200;
				final chunk = [];
				while (chunk.length != chunkLen) {
					try {
						chunk.push(f.readLine());
                        loaded++;
					} catch (e:Eof) {
						break;
					}
				}
				queue.add(chunk);
				queuedChunks++;
				pool.run(() -> {
					trace('Starting queued process');
					final lines = queue.pop(true);
					trace("Grabbed lines from queue");
					mutex.acquire();
					dbc.exec('BEGIN TRANSACTION;').then((_) -> {
						trace("Transaction started");
						return;
					}, (e:SqliteError) -> {
						trace('DB Transaction error', e.name, e.message);
						return;
					});
					for (line in lines) {
						if (line.startsWith('#'))
							continue;
						final vals = line.split(' ');
						dbc.exec('INSERT INTO hosts (ip, port, ts) VALUES ("${vals[3]}", ${vals[2]}, ${vals[4]})').then((_) -> {
							inserted++;
							trace("Inserted value");
							return;
						}, (e:SqliteError) -> {
							trace('DB Insert error', e.name, e.message);
							return;
						});
					}
					dbc.exec('COMMIT;').then((_) -> {
						trace("DB Committed");
						mutex.release();
						queuedChunks--;
						return 0;
					}, (e:SqliteError) -> {
						trace('DB Commit error', e.name, e.message);
						return 0;
					});
				});
			}
		});
		do {
			final insertedPerSec = (inserted - delta_inserted) * 100;
            delta_inserted = inserted;
            final loadedPerSec = (loaded - delta_loaded) * 100;
            delta_loaded = loaded;
            Sys.print([for (_ in 0...110) ' '].join('')+'\r');
			Sys.print('${Math.round((Sys.time() - start) * 100) / 100}\t[${Math.round((inserted/maxlines)*1000)/100}%]\t>\tLoad:${loadedPerSec}/s\t<->\tInsert:${insertedPerSec}/s\t(${queuedChunks} chunks queued, ${inserted} lines done)\r');
			Sys.sleep(0.01);
		} while (pool.threadsCount != 0);
        dbc.close();
		Sys.println('\nFinished processing in ${Sys.time() - start}s');
	}
}
