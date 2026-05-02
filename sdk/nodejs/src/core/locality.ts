import * as net from 'net';
import * as fs from 'fs';
import { Table as ArrowTable, tableFromIPC } from 'apache-arrow';
import { FlightClient } from 'apache-arrow/flight';

export enum RouteType {
    LOCAL = 'LOCAL',
    REMOTE = 'REMOTE'
}

export interface FlightTicket {
    route_type: RouteType;
    address: string;
    resource_id: string;
}

/**
 * Resolves a FlightTicket to an Apache Arrow Table.
 * Implements the Dual-Path logic for LOCAL (UDS + FD) and REMOTE (Flight).
 */
export async function resolveTicket(ticket: FlightTicket): Promise<ArrowTable> {
    if (ticket.route_type === RouteType.LOCAL) {
        return resolveLocal(ticket);
    } else if (ticket.route_type === RouteType.REMOTE) {
        return resolveRemote(ticket);
    } else {
        throw new Error(`Unknown route type: ${ticket.route_type}`);
    }
}

async function resolveLocal(ticket: FlightTicket): Promise<ArrowTable> {
    const socketPath = ticket.address.replace('unix://', '');
    
    return new Promise((resolve, reject) => {
        const client = net.createConnection(socketPath, () => {
            // 1. Send ResourceID
            client.write(ticket.resource_id);
        });

        client.on('error', reject);

        // Note: Receiving raw file descriptors via SCM_RIGHTS in Node.js 
        // typically requires a native module (like 'pass-fd') as the built-in 
        // 'net' module only supports passing handles (sockets/servers) 
        // between Node processes.
        
        // This is a placeholder for the FD reception logic.
        // In a production Heddle environment, we use a native binding.
        let fd: number | null = null;
        
        // Hypothetical event from a native bridge
        (client as any).on('fileDescriptor', (receivedFd: number) => {
            fd = receivedFd;
            
            try {
                // 2. mmap the FD (using fs.readFileSync as a proxy for the buffer)
                // In Phase 5, we use a proper mmap-io call here.
                const stats = fs.fstatSync(fd);
                const buffer = Buffer.alloc(stats.size);
                fs.readSync(fd, buffer, 0, stats.size, 0);
                
                // 3. Open Arrow Table from buffer
                const table = tableFromIPC(buffer);
                resolve(table);
            } catch (e) {
                reject(e);
            } finally {
                if (fd !== null) fs.closeSync(fd);
                client.end();
            }
        });

        client.on('data', (data) => {
            const msg = data.toString();
            if (msg !== 'OK' && !fd) {
                reject(new Error(`Worker returned error: ${msg}`));
            }
        });
    });
}

async function resolveRemote(ticket: FlightTicket): Promise<ArrowTable> {
    const addr = ticket.address.replace('grpc://', '');
    
    // 1. Connect to peer
    const client = new FlightClient(`grpc://${addr}`);
    
    // 2. DoGet
    const reader = await client.doGet(Buffer.from(ticket.resource_id));
    
    // 3. Consume stream into Table
    const table = await reader.readAll();
    return table;
}
