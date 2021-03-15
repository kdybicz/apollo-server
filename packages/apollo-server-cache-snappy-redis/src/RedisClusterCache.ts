import { KeyValueCache, KeyValueCacheSetOptions } from 'apollo-server-caching';
import Redis, {
  ClusterOptions,
  ClusterNode,
  Redis as RedisInstance,
} from 'ioredis';
import DataLoader from 'dataloader';
import snappy from 'snappy';

export class RedisClusterCache implements KeyValueCache {
  readonly client: any;
  readonly defaultSetOptions: KeyValueCacheSetOptions = {
    ttl: 300,
  };
  private minimumCompressionSize = 262144;

  private loader: DataLoader<string, string | null>;

  constructor(nodes: ClusterNode[], options?: ClusterOptions) {
    const client = this.client = new Redis.Cluster(nodes, options);

    this.loader = new DataLoader(
      (keys = []) =>
        Promise.all(keys.map(key => client.get(key).catch(() => null))),
      { cache: false },
    );
  }

  async set(
    key: string,
    data: string,
    options?: KeyValueCacheSetOptions,
  ): Promise<void> {
    const { ttl } = Object.assign({}, this.defaultSetOptions, options);
    if (data.length > this.minimumCompressionSize) {
      data = 'snappy:' + snappy.compressSync(data).toString();
    }
    if (typeof ttl === 'number') {
      await this.client.set(key, data, 'EX', ttl);
    } else {
      // We'll leave out the EXpiration when no value is specified.  Of course,
      // it may be purged from the cache for other reasons as deemed necessary.
      await this.client.set(key, data);
    }
  }

  async get(key: string): Promise<string | undefined> {
    const reply = await this.loader.load(key);
    // reply is null if key is not found
    if (reply !== null) {
      if (reply !== undefined && reply.startsWith('snappy:')) {
        return snappy.uncompressSync(Buffer.from(reply.slice(7)), { asBuffer: false }) as string;
      }
      return reply;
    }
    return;
  }

  async delete(key: string): Promise<boolean> {
    return await this.client.del(key);
  }

  async flush(): Promise<void> {
    const masters = this.client.nodes('master') || [];
    await Promise.all(masters.map((node: RedisInstance) => node.flushdb()));
  }

  async close(): Promise<void> {
    await this.client.quit();
    return;
  }
}
