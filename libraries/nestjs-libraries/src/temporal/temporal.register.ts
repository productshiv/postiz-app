import { Global, Injectable, Logger, Module, OnModuleInit } from '@nestjs/common';
import { TemporalService } from 'nestjs-temporal-core';
import { Connection } from '@temporalio/client';

@Injectable()
export class TemporalRegister implements OnModuleInit {
  private readonly _logger = new Logger(TemporalRegister.name);

  constructor(private _client: TemporalService) {}

  async onModuleInit(): Promise<void> {
    if (process.env.TEMPORAL_TLS === 'true') {
      return;
    }
    const connection = this._client?.client?.getRawClient()
      ?.connection as Connection;

    // Temporal only becomes reachable after its own dependencies (Postgres,
    // Elasticsearch) are ready, which can take longer than this service.
    // Retry instead of crashing so a slow Temporal start-up does not exhaust
    // the container restart limit.
    const maxAttempts = 30;
    for (let attempt = 1; ; attempt++) {
      try {
        await this._registerSearchAttributes(connection);
        return;
      } catch (err) {
        if (attempt >= maxAttempts) {
          throw err;
        }
        this._logger.warn(
          `Temporal not ready (attempt ${attempt}/${maxAttempts}), retrying in 5s...`
        );
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  private async _registerSearchAttributes(
    connection: Connection
  ): Promise<void> {
    const { customAttributes } =
      await connection.operatorService.listSearchAttributes({
        namespace: process.env.TEMPORAL_NAMESPACE || 'default',
      });

    const neededAttribute = ['organizationId', 'postId'];
    const missingAttributes = neededAttribute.filter(
      (attr) => !customAttributes[attr]
    );

    if (missingAttributes.length > 0) {
      await connection.operatorService.addSearchAttributes({
        namespace: process.env.TEMPORAL_NAMESPACE || 'default',
        searchAttributes: missingAttributes.reduce((all, current) => {
          // @ts-ignore
          all[current] = 1;
          return all;
        }, {}),
      });
    }
  }
}

@Global()
@Module({
  imports: [],
  controllers: [],
  providers: [TemporalRegister],
  get exports() {
    return this.providers;
  },
})
export class TemporalRegisterMissingSearchAttributesModule {}
