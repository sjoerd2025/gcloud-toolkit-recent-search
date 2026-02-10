const { insertTweets } = require('./bq-search-results');
const { BigQuery } = require('@google-cloud/bigquery');
const config = require('../config');

jest.mock('@google-cloud/bigquery', () => {
  const mTable = {
    insert: jest.fn().mockResolvedValue([]),
  };
  const mDataset = {
    table: jest.fn().mockReturnValue(mTable),
  };
  const mBigQuery = jest.fn(() => ({
    dataset: jest.fn().mockReturnValue(mDataset),
  }));
  mBigQuery.datetime = jest.fn((val) => `DATETIME(${val})`);
  return { BigQuery: mBigQuery };
});

describe('insertTweets', () => {
  let reqBody;
  let mockBigQueryInstance;

  beforeEach(() => {
    jest.clearAllMocks();
    reqBody = {
      recentSearch: {
        category: 'test-category',
        subCategory: 'test-subcategory'
      },
      dataSet: {
        dataSetName: 'test-dataset'
      }
    };
    // The instance created in the module
    mockBigQueryInstance = new BigQuery();
  });

  test('should correctly format a tweet with all fields', async () => {
    const data = [{
      id: '123',
      text: 'hello world',
      created_at: '2023-01-01T00:00:00Z',
      source: 'twitter',
      author_id: 'auth123',
      conversation_id: 'conv123',
      lang: 'en',
      possibly_sensitive: false,
      context_annotations: [{ domain: { name: 'test' } }],
      referenced_tweets: [{ type: 'replied_to', id: '456' }],
      entities: { hashtags: [] },
      public_metrics: { retweet_count: 10 },
      geo: { place_id: 'geo123' },
      withheld: { copyright: false }
    }];

    await insertTweets(data, reqBody);

    const bq = mockBigQueryInstance;
    const dataset = bq.dataset('test-dataset');
    const table = dataset.table(config.bq.table.tweets);

    expect(bq.dataset).toHaveBeenCalledWith('test-dataset');
    expect(dataset.table).toHaveBeenCalledWith(config.bq.table.tweets);

    const insertedRows = table.insert.mock.calls[0][0];
    expect(insertedRows[0].id).toBe('123');
    expect(insertedRows[0].text).toBe('hello world');
    expect(insertedRows[0].category).toBe('test-category');
    expect(insertedRows[0].subcategory).toBe('test-subcategory');
    expect(insertedRows[0].created_at).toBe('DATETIME(2023-01-01T00:00:00.000Z)');
    expect(insertedRows[0].context_annotations).toEqual([{ domain: { name: 'test' } }]);
    expect(insertedRows[0].referenced_tweets).toEqual([{ type: 'replied_to', id: '456' }]);
  });

  test('should set context_annotations and referenced_tweets to empty array if undefined', async () => {
    const data = [{
      id: '123',
      text: 'hello world',
      created_at: '2023-01-01T00:00:00Z',
      // context_annotations and referenced_tweets are missing
    }];

    await insertTweets(data, reqBody);

    const bq = mockBigQueryInstance;
    const table = bq.dataset().table();
    const insertedRows = table.insert.mock.calls[0][0];
    expect(insertedRows[0].context_annotations).toEqual([]);
    expect(insertedRows[0].referenced_tweets).toEqual([]);
  });

  test('should handle multiple tweets', async () => {
    const data = [
      { id: '1', text: 'tweet 1', created_at: '2023-01-01T00:00:00Z' },
      { id: '2', text: 'tweet 2', created_at: '2023-01-01T00:00:01Z' }
    ];

    await insertTweets(data, reqBody);

    const bq = mockBigQueryInstance;
    const table = bq.dataset().table();
    const insertedRows = table.insert.mock.calls[0][0];
    expect(insertedRows).toHaveLength(2);
    expect(insertedRows[0].id).toBe('1');
    expect(insertedRows[1].id).toBe('2');
  });

  test('should skip null or undefined tweets in the array', async () => {
    const data = [
      { id: '1', text: 'tweet 1', created_at: '2023-01-01T00:00:00Z' },
      null,
      undefined,
      { id: '2', text: 'tweet 2', created_at: '2023-01-01T00:00:01Z' }
    ];

    await insertTweets(data, reqBody);

    const bq = mockBigQueryInstance;
    const table = bq.dataset().table();
    const insertedRows = table.insert.mock.calls[0][0];
    expect(insertedRows).toHaveLength(2);
  });
});
