export interface Edge {
  source: string;
  target: string;
}

export interface SearchParams {
  author_did?: string;
  author_handle?: string;
  post?: string;
  selectdPost?: string;
  selectedAuthor?: string;
}

export interface Post {
  id: string;
  text: string;
  parent_post_id: string | null;
  root_post_id: string | null;
  author_did: string;
  created_at: string;
  has_embedded_media: boolean;
}

export interface ThreadItem {
  key: string;
  author_handle: string;
  post: Post;
  depth: number;
}

export interface SelectedNode {
  id: string;
  text: string;
  created_at: string;
  author_handle: string;
  author_did: string;
  has_media: boolean;
  x: number;
  y: number;
}
