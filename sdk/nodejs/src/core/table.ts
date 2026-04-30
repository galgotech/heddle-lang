export class Table {
    // Encapsulate apache-arrow memory buffers or just basic JSON for now.
    // In the future this should be extended to support arrow buffers zero-copy.
    private _data: any;
    private _buffer: Buffer | null = null;

    constructor(data?: any) {
        this._data = data;
    }

    get data(): any {
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
        try {
            // Placeholder: Parse Arrow buffer or JSON buffer
            const jsonStr = buffer.toString('utf-8');
            if (jsonStr) {
                table._data = JSON.parse(jsonStr);
            }
        } catch (e) {
            // Not json, maybe arrow or empty
        }
        return table;
    }

    toBuffer(): Buffer {
        if (this._buffer) {
            return this._buffer;
        }
        if (this._data) {
            return Buffer.from(JSON.stringify(this._data));
        }
        return Buffer.alloc(0);
    }
}
