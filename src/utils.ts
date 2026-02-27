export const getDocPrefix = (namespace: string) => {
  return [namespace, "doc"] as const satisfies Deno.KvKey;
};

export const getViewRefPrefix = (namespace: string, viewName: string) => {
  return [namespace, "viewref", viewName] as const satisfies Deno.KvKey;
};

export const getDocumentKey = (namespace: string, id: string) => {
  return [...getDocPrefix(namespace), id] as const satisfies Deno.KvKey;
};

export const getDesignKey = (namespace: string, signature: string) => {
  return [namespace, "design", signature] as const satisfies Deno.KvKey;
};

export const getViewKey = (
  namespace: string,
  viewName: string,
  signature: string,
) => {
  return [namespace, "view", viewName, signature] as const satisfies Deno.KvKey;
};

export const getViewRefKey = (
  namespace: string,
  viewName: string,
  docId: string,
) => {
  return [
    ...getViewRefPrefix(namespace, viewName),
    docId,
  ] as const satisfies Deno.KvKey;
};
