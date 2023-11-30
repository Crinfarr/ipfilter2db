import sys.io.FileInput;
import sqlite.SqliteError;
import haxe.io.Eof;
import haxe.Exception;
import haxe.macro.Expr.Catch;
import sys.thread.ElasticThreadPool;
import haxe.Timer;
import sys.thread.EventLoop;
import sys.thread.Mutex;
import sys.thread.Thread;
import sys.thread.Deque;
import sqlite.Database;
import sys.io.File;

using StringTools;

class Main {
    static function main() {
        final noconcurrenttimers = new Mutex();
        final db = new Database('hosts.db', ReadWrite);
        db.open();
        Timer.measure(() -> {
            noconcurrenttimers.acquire();
            process_Threaded(db, File.read(Sys.args()[0], false), 24, 200);
            // db.exec('DELETE FROM hosts;').then((_) -> {
            //     noconcurrenttimers.release();
            // });
        });
        noconcurrenttimers.acquire();
        // Timer.measure(() -> {
        //     noconcurrenttimers.acquire();
        //     process_Unthreaded(db, File.read(Sys.args()[0], false), 200);
        //     db.exec('DELETE FROM hosts;').then((_) -> {
        //         noconcurrenttimers.release();
        //     });
        // });
        // noconcurrenttimers.acquire();
    }
	static function process_Threaded(db:Database, ifile:FileInput, threadCount:Int, chunkSize:Int) {
		var pool = new ElasticThreadPool(threadCount);
		var queue = new Deque<Array<String>>();
		var mutex = new Mutex();
		var running = 0;
		while (!ifile.eof()) {
			var chunk = [];
			while (chunk.length != chunkSize) {
				try {
					chunk.push(ifile.readLine());
				} catch (e:Eof) {
					break;
				}
			}
			queue.push(chunk);
			pool.run(() -> {
				running++;
				var lines = queue.pop(true);
				
                mutex.acquire();
				db.exec("BEGIN TRANSACTION;").then(_ -> {}, (error:SqliteError) -> {
					trace("ERROR1", error.message, error.name);
				});

				for (line in lines) {
                    if (line.startsWith('#'))
                        continue;
					var parts = line.split(" ");

					db.exec('INSERT INTO hosts (ip, port, ts) VALUES ("${parts[3]}", ${parts[2]}, ${parts[4]})').then(_ -> {}, (error:SqliteError) -> {
						trace("ERROR2", error.message, error.name);
					});
				}

				db.exec("COMMIT;").then(_ -> {
					running--;
					mutex.release();
                }, (error:SqliteError) -> {
					trace("ERROR1", error.message, error.name);
				});
				db.close();
			});
		}

        ifile.close();

		while (running > 0) {
			Sys.sleep(.1);
		}
	}
    static function process_Unthreaded(db:Database, ifile:FileInput, chunkSize:Int) {
        while(!ifile.eof()) {
            var chunk = [];
            while (chunk.length != chunkSize) {
                try {
                    chunk.push(ifile.readLine());
                } catch(e:Eof) {
                    break;
                }
            }
            db.exec('BEGIN TRANSACTION;');
            for (line in chunk) {
                if (line.startsWith('#'))
                    continue;
                final parts = line.split(' ');
				db.exec('INSERT INTO hosts (ip, port, ts) VALUES ("${parts[3]}", ${parts[2]}, ${parts[4]})');
            }
            db.exec('COMMIT');
        }
    }
}