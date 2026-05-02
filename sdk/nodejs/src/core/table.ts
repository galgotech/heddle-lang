import { Table as ArrowTable, tableToIPC } from 'apache-arrow';

export class Table {
    private _data: ArrowTable | null = null;
    private _buffer: Buffer | null = null;

    constructor(data?: ArrowTable) {
        if (data) {
            this._data = data;
        }
    }

    get native(): ArrowTable | null {
        return this._data;
    }

    get buffer(): Buffer | null {
        return this._buffer;
    }

    set buffer(b: Buffer | null) {
        this._buffer = b;
    }

    static fromBuffer(buffer: Buffer): Table {
        const table = new Table();
        table.buffer = buffer;
        return table;
    }

    toBuffer(): Buffer {
        if (this._buffer) {
            return this._buffer;
        }
        if (this._data) {
            // Serialize to Arrow IPC
            return Buffer.from(tableToIPC(this._data));
        }
        return Buffer.alloc(0);
    }

    get numRows(): number {
        return this._data ? this._data.numRows : 0;
    }
}
