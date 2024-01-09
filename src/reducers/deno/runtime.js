import("lyra:reducer").then(({ reduce }) => {
  globalThis["reduce"] = reduce;
});
