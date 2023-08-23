import { FC, useEffect, useState } from "react";
import ErrorMsg from "../ErrorMsg/ErrorMsg";
import { Link, useParams } from "react-router-dom";
import { HandThumbUpIcon, UserIcon } from "@heroicons/react/24/solid";

interface Image {
  cid: string;
  url: string;
  alt: string;
}

interface Post {
  actor_did: string;
  actor_handle?: string;
  actor_propic_url?: string;
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
    setPost(undefined);
    fetchPost();
  }, [handleOrDid, rkey]);

  return (
    <div className="mx-auto max-w-2xl">
      {error && (
        <div className="text-left mb-2">
          <ErrorMsg error={error} />
        </div>
      )}
      <div className="text-center pt-2 sm:pt-6">
        <h3 className="text-xl leading-6 font-medium text-gray-100">
          Pubsky Thread Viewer
        </h3>
      </div>
      <div className="mt-2 sm:mt-8 flow-root">
        <div className="overflow-hidden shadow ring-1 ring-black ring-opacity-5 sm:rounded-lg pb-5 bg-gray-700 mb-4">
          {post && (
            <div className="flow-root sm:px-4">
              <div className="px-6 py-4 mt-4 pb-8">
                <div className="flex items-center">
                  <div className="flex-shrink-0">
                    {(post?.actor_propic_url && (
                      <img
                        className="h-10 w-10 rounded-full"
                        src={post?.actor_propic_url || ""}
                      />
                    )) || (
                      <div className="flex h-11 w-11 items-center justify-center rounded-full bg-gray-600 text-white">
                        <UserIcon />
                      </div>
                    )}
                  </div>
                  <div className="ml-3 min-w-0 flex-1 inline-flex ">
                    <p className="text-sm font-medium text-gray-100 truncate">
                      <a
                        href={`https://bsky.app/profile/${post?.actor_did}`}
                        target="_blank"
                      >
                        {post?.actor_handle}
                      </a>
                    </p>
                  </div>
                </div>
                <div>
                  <div className="ml-3 flex-1">
                    {post?.images && post?.images?.length > 0 && (
                      <div className="flex flex-wrap mr-0 gap-2 mt-2">
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
                    )}
                    <p className="text-sm text-gray-100 mt-2">
                      {post?.content
                        ?.split("\n")
                        .map((line, idx) => <p key={idx}>{line}</p>) || ""}
                    </p>
                  </div>
                  <div className="flex justify-between mt-2 ml-2">
                    <span className="inline-flex px-3 py-0.5 rounded-full text-sm font-medium bg-blue-100 text-blue-800">
                      <HandThumbUpIcon
                        className="-ml-1 mr-0.5 h-5 w-5 text-blue-500"
                        aria-hidden="true"
                      />
                      {post?.like_count.toLocaleString()}
                    </span>
                    <p className="text-xs text-gray-300 mt-auto mb-auto">
                      {post?.created_at
                        ? new Date(post?.created_at).toLocaleString()
                        : ""}
                    </p>
                  </div>
                </div>
              </div>
              {post?.replies && post?.replies?.length > 0 && (
                <div className="block">
                  <div className="pb-4 px-4">
                    <div className="border-gray-800 border-t-2" />
                  </div>
                </div>
              )}
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
                            {(reply?.actor_propic_url && (
                              <img
                                className="flex h-11 w-11 items-center justify-center rounded-full bg-gray-600"
                                src={reply?.actor_propic_url || ""}
                              />
                            )) || (
                              <div className="flex h-11 w-11 items-center justify-center rounded-full bg-gray-600 text-white">
                                <UserIcon />
                              </div>
                            )}
                          </div>
                          <div className="min-w-0 flex-1">
                            <div className="inline-flex justify-between">
                              <div className="text-sm truncate text-gray-100 font-semibold">
                                <a
                                  href={`https://bsky.app/profile/${reply?.actor_did}`}
                                  target="_blank"
                                >
                                  {reply?.actor_handle}
                                </a>
                              </div>
                            </div>
                            {reply?.images && reply?.images?.length > 0 && (
                              <div className="flex flex-wrap mr-12 mt-2 gap-2">
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
                            <div className="flex justify-between mr-4 mt-2">
                              <span className="inline-flex px-3 py-0.5 rounded-full text-sm font-medium bg-blue-100 text-blue-800">
                                <HandThumbUpIcon
                                  className="-ml-1 mr-0.5 h-5 w-5 text-blue-500"
                                  aria-hidden="true"
                                />
                                {reply?.like_count.toLocaleString()}
                              </span>
                              <p className="my-auto text-xs text-gray-300 hover:underline">
                                <Link
                                  to={`/profile/${reply?.actor_did}/post/${reply?.rkey}`}
                                >
                                  {reply?.created_at
                                    ? new Date(
                                        reply?.created_at
                                      ).toLocaleString()
                                    : ""}
                                </Link>
                              </p>
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
