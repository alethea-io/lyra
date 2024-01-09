import("lyra:reducer").then(({ apply, undo }) => {
  globalThis["apply"] = apply;
  globalThis["undo"] = undo;
});