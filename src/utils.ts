export const getDocPrefix = (namespace: string) => {
  return [namespace, "doc"] as const satisfies Deno.KvKey;
};

export const getViewRefPrefix = (namespace: string, viewName: string) => {
  return [namespace, "viewref", viewName] as const satisfies Deno.KvKey;
};

export const getDocumentKey = (namespace: string, id: string) => {
  return [...getDocPrefix(namespace), id] as const satisfies Deno.KvKey;
};

export const getDesignKey = (namespace: string, id: string) => {
  return [namespace, "design", id] as const satisfies Deno.KvKey;
};

export const getViewKey = (namespace: string, viewName: string) => {
  return [namespace, "view", viewName] as const satisfies Deno.KvKey;
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
