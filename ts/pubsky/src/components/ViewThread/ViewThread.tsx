import { FC, useEffect, useState } from "react";
import ErrorMsg from "../ErrorMsg/ErrorMsg";
import { useParams } from "react-router-dom";
import { HandThumbUpIcon } from "@heroicons/react/24/solid";

interface Image {
  cid: string;
  url: string;
  alt: string;
}

interface Post {
  actor_did: string;
  actor_handle?: string;
  rkey: string;
  content: string;
  parent_post_actor_did?: string;
  parent_post_rkey?: string;
  root_post_actor_did?: string;
  root_post_rkey?: string;
  quote_post_actor_did?: string;
  quote_post_rkey?: string;
  has_embedded_media: boolean;
  created_at: string;
  inserted_at: string;
  like_count: number;
  images?: Image[];
  replies?: (Post | undefined)[];
}

interface PostResponse {
  post: Post;
  error: string;
}

const FeedList: FC<{}> = () => {
  const { handleOrDid, rkey } = useParams<{
    handleOrDid: string;
    rkey: string;
  }>();
  const [error, setError] = useState<string>("");
  const [post, setPost] = useState<Post | undefined>(undefined);

  useEffect(() => {
    document.title = "Pubsky | View Thread";
  }, []);

  const fetchPost = () => {
    fetch(`http://10.0.6.32:9003/api/v1/profile/${handleOrDid}/post/${rkey}`)
      .then((res) => res.json())
      .then((res: PostResponse) => {
        if (res.error) {
          setError(res.error);
        } else {
          setPost(res.post);
        }
      })
      .catch((err) => {
        setError(err.message);
      });
  };

  useEffect(() => {
    fetchPost();
  }, []);

  return (
    <div className="mx-auto max-w-2xl">
      {error && (
        <div className="text-left mb-2">
          <ErrorMsg error={error} />
        </div>
      )}
      <div className="mt-8 flow-root">
        <div className="overflow-hidden shadow ring-1 ring-black ring-opacity-5 sm:rounded-lg pb-5 bg-gray-700">
          {post && (
            <div className="flow-root px-4">
              <div className="px-6 py-4 mt-4 pb-8">
                <div className="flex items-center">
                  <div className="flex-shrink-0">
                    <img
                      className="h-10 w-10 rounded-full"
                      src={post?.images?.[0].url}
                      alt={post?.images?.[0].alt}
                    />
                  </div>
                  <div className="ml-3 min-w-0 flex-1 inline-flex justify-between">
                    <p className="text-sm font-medium text-gray-100 truncate">
                      <a
                        href={`https://bsky.app/profile/${post?.actor_did}`}
                        target="_blank"
                      >
                        {post?.actor_handle}
                      </a>
                    </p>

                    <p className="text-xs text-gray-300">
                      {post?.created_at
                        ? new Date(post?.created_at).toLocaleString()
                        : ""}
                    </p>
                  </div>
                </div>
                <div className="mt-4">
                  <div className="ml-3 flex-1">
                    <div className="flex flex-wrap mr-12">
                      {post?.images?.map((image, idx) => (
                        <div key={idx}>
                          <img
                            className="h-auto max-w-full rounded-lg"
                            src={image.url}
                            alt={image.alt}
                          />
                        </div>
                      ))}
                    </div>
                    <p className="text-sm text-gray-100">
                      {post?.content
                        ?.split("\n")
                        .map((line, idx) => <p key={idx}>{line}</p>) || ""}
                    </p>
                  </div>
                  <div className="ml-auto flex justify-end">
                    <span className="inline-flex px-3 py-0.5 rounded-full text-sm font-medium bg-blue-100 text-blue-800">
                      <HandThumbUpIcon
                        className="-ml-1 mr-0.5 h-5 w-5 text-blue-500"
                        aria-hidden="true"
                      />
                      {post?.like_count.toLocaleString()}
                    </span>
                  </div>
                </div>
              </div>
              <ul role="list" className="-mb-8">
                {post.replies?.map((reply, replyIdx) => (
                  <li key={reply?.actor_did + "-" + reply?.rkey}>
                    <div className="relative pb-8">
                      {replyIdx !== (post.replies?.length || 0) - 1 ? (
                        <span
                          className="absolute left-8 top-5 -ml-px h-full w-0.5 bg-gray-400"
                          aria-hidden="true"
                        />
                      ) : null}
                      <div className="relative flex items-start space-x-3 ml-3">
                        <>
                          <div className="relative">
                            <img
                              className="flex h-11 w-11 items-center justify-center rounded-full bg-gray-600"
                              src={`https://cdn.bsky.social/imgproxy/7LvKFeY0otwnAW11x2UcLeTL__m_WDqZgw8BHJT59tY/rs:fill:1000:1000:1:0/plain/bafkreigio6g2gv3phaprfjukxpwytdyxyic45reldeptypywkt7xrhi3h4@jpeg`}
                              alt={reply?.images?.[0].alt}
                            />
                          </div>
                          <div className="min-w-0 flex-1">
                            <div className="inline-flex justify-between w-full">
                              <div className="text-sm">
                                <a
                                  href={`https://bsky.app/profile/${reply?.actor_did}`}
                                  target="_blank"
                                  className="font-semibold text-gray-100"
                                >
                                  {reply?.actor_handle}
                                </a>
                              </div>
                              <div>
                                <p className="mt-0.5 text-xs text-gray-300 mr-4">
                                  <a
                                    href={`/profile/${reply?.actor_did}/post/${reply?.rkey}`}
                                  >
                                    {reply?.created_at
                                      ? new Date(
                                          reply?.created_at
                                        ).toLocaleString()
                                      : ""}
                                  </a>
                                </p>
                              </div>
                            </div>
                            {reply?.images && reply?.images?.length > 0 && (
                              <div className="flex flex-wrap mr-12 mt-2">
                                {reply?.images?.map((image, idx) => (
                                  <div key={idx}>
                                    <img
                                      className="h-auto max-w-full rounded-lg"
                                      src={image.url}
                                      alt={image.alt}
                                    />
                                  </div>
                                ))}
                              </div>
                            )}
                            <div className="mt-2 text-sm text-gray-100 mr-4">
                              {reply?.content
                                ?.split("\n")
                                .map((line, idx) => <p key={idx}>{line}</p>) ||
                                ""}
                            </div>
                            <div className="flex justify-end mr-4">
                              <span className="inline-flex px-3 py-0.5 rounded-full text-sm font-medium bg-blue-100 text-blue-800">
                                <HandThumbUpIcon
                                  className="-ml-1 mr-0.5 h-5 w-5 text-blue-500"
                                  aria-hidden="true"
                                />
                                {reply?.like_count.toLocaleString()}
                              </span>
                            </div>
                          </div>
                        </>
                      </div>
                    </div>
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default FeedList;
