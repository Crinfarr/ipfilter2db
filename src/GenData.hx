package ;
import sys.io.File;

class GenData {
    static function main() {
        final f = File.write('testdata.list', false);
        for (_line in 0...200000) {
			f.writeString('open tcp ${Std.random(0x10000)} ${randip()} ${Math.round(Date.now().getTime())}\n');
        }
        f.close();
    }
    static function randip():String {
		return '${Std.random(256)}.${Std.random(256)}.${Std.random(256)}.${Std.random(256)}';
    }
}