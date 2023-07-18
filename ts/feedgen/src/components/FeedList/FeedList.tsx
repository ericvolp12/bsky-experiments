import { FC, useEffect, useState } from "react";
import ErrorMsg from "../ErrorMsg/ErrorMsg";
import { ChevronDownIcon, ChevronUpIcon } from "@heroicons/react/24/solid";

interface Feed {
  feed_name?: string;
  feed_type: string;
  user_count: number;
}

type StatusKey = keyof Feed;

type FeedMap = { [key: string]: Feed };

interface FeedResponse {
  feeds: FeedMap;
}

const FeedList: FC<{}> = () => {
  const [feeds, setFeeds] = useState<Feed[] | null>(null);
  const [error, setError] = useState<string>("");
  const [sortField, setSortField] = useState<StatusKey>("feed_name");
  const [sortOrder, setSortOrder] = useState<string>("asc");

  useEffect(() => {
    document.title = "Feed Generator Admin Dashboard";
  }, []);

  const refreshFeeds = () => {
    fetch("https://feedsky.jazco.io/admin/feeds")
      .then((res) => res.json())
      .then((res: FeedResponse) => {
        const feedList: Feed[] = [];
        Object.keys(res.feeds).forEach((feedName) => {
          const feed = res.feeds[feedName];
          feedList.push({ feed_name: feedName, ...feed });
        });
        setFeeds(feedList);
      })
      .catch((err) => {
        setError(err.message);
      });
  };

  useEffect(() => {
    if (!feeds) {
      return;
    }
    const sortedFeeds: Feed[] = [...feeds].sort((a, b) => {
      if (sortOrder === "asc") {
        if (a[sortField]! < b[sortField]!) {
          return -1;
        }
        if (a[sortField]! > b[sortField]!) {
          return 1;
        }
      } else {
        if (a[sortField]! < b[sortField]!) {
          return 1;
        }
        if (a[sortField]! > b[sortField]!) {
          return -1;
        }
      }
      return 0;
    });
    setFeeds(sortedFeeds);
  }, [sortOrder, sortField]);

  useEffect(() => {
    refreshFeeds();
    // // Refresh stats every 5 minutes
    // const interval = setInterval(() => {
    //   refreshFeeds();
    // }, 5 * 60 * 1000);

    // return () => clearInterval(interval);
  }, []);

  return (
    <div className="mx-auto max-w-full">
      {error && (
        <div className="text-left mb-2">
          <ErrorMsg error={error} />
        </div>
      )}
      <div className="sm:flex sm:items-center">
        <div className="sm:flex-auto">
          <h1 className="text-2xl font-semibold leading-6 text-gray-900">
            Feed Generators
          </h1>
          <p className="mt-2 text-sm text-gray-700">
            A list of Feed Aliases, their backing Generators, and the current
            number of users subscribed to each.
          </p>
        </div>
      </div>
      <div className="mt-8 flow-root">
        <div className="overflow-hidden shadow ring-1 ring-black ring-opacity-5 sm:rounded-lg sm:rounded-b-none">
          <table className="min-w-full divide-y divide-gray-300">
            <thead className="bg-gray-50">
              <tr>
                <th
                  scope="col"
                  className="py-3.5 pl-4 pr-3 text-left text-sm font-semibold text-gray-900 sm:pl-6"
                >
                  <a
                    href="#"
                    className="group inline-flex"
                    onClick={() => {
                      setSortField("feed_name");
                      setSortOrder(sortOrder === "asc" ? "desc" : "asc");
                    }}
                  >
                    Feed Name
                    <span
                      className={`ml-2 flex-none rounded text-gray-400 ${
                        sortField === "feed_name"
                          ? "group-hover:bg-gray-200"
                          : "invisible group-hover:visible group-focus:visible"
                      }`}
                    >
                      {sortField === "feed_name" && sortOrder === "asc" ? (
                        <ChevronUpIcon className="h-5 w-5" aria-hidden="true" />
                      ) : (
                        <ChevronDownIcon
                          className="h-5 w-5"
                          aria-hidden="true"
                        />
                      )}
                    </span>
                  </a>
                </th>
                <th
                  scope="col"
                  className="px-3 py-3.5 text-left text-sm font-semibold text-gray-900"
                >
                  <a
                    href="#"
                    className="group inline-flex"
                    onClick={() => {
                      setSortField("feed_type");
                      setSortOrder(sortOrder === "asc" ? "desc" : "asc");
                    }}
                  >
                    Generator Type
                    <span
                      className={`ml-2 flex-none rounded text-gray-400 ${
                        sortField === "feed_type"
                          ? "group-hover:bg-gray-200"
                          : "invisible group-hover:visible group-focus:visible"
                      }`}
                    >
                      {sortField === "feed_type" && sortOrder === "asc" ? (
                        <ChevronUpIcon className="h-5 w-5" aria-hidden="true" />
                      ) : (
                        <ChevronDownIcon
                          className="h-5 w-5"
                          aria-hidden="true"
                        />
                      )}
                    </span>
                  </a>
                </th>
                <th
                  scope="col"
                  className="px-3 py-3.5 text-right text-sm font-semibold text-gray-900 pr-6 whitespace-nowrap"
                >
                  <a
                    href="#"
                    className="group inline-flex"
                    onClick={() => {
                      setSortField("user_count");
                      setSortOrder(sortOrder === "asc" ? "desc" : "asc");
                    }}
                  >
                    User Count
                    <span
                      className={`ml-2 flex-none rounded text-gray-400 ${
                        sortField === "user_count"
                          ? "group-hover:bg-gray-200"
                          : "invisible group-hover:visible group-focus:visible"
                      }`}
                    >
                      {sortField === "user_count" && sortOrder === "asc" ? (
                        <ChevronUpIcon className="h-5 w-5" aria-hidden="true" />
                      ) : (
                        <ChevronDownIcon
                          className="h-5 w-5"
                          aria-hidden="true"
                        />
                      )}
                    </span>
                  </a>
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200 bg-white">
              {feeds &&
                feeds.map((feed) => {
                  return (
                    <tr key={feed.feed_name}>
                      <td className="whitespace-nowrap py-4 pl-4 pr-3 text-sm font-medium text-gray-900 sm:pl-6 text-left">
                        {feed.feed_name}
                      </td>
                      <td className="whitespace-nowrap px-3 py-4 text-sm text-gray-500 text-left">
                        {feed.feed_type}
                      </td>
                      <td className="whitespace-nowrap px-3 py-2 text-sm text-gray-400 text-right w-8 pr-6">
                        {feed.user_count}
                      </td>
                    </tr>
                  );
                })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default FeedList;
