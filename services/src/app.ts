import * as restate from "@restatedev/restate-sdk";

const get = async (ctx: restate.ObjectContext) => {
  return await ctx.get("value");
};

const set = async (ctx: restate.ObjectContext, value: object) => {
  ctx.console.log({
    msg: "Updating value",
    newValue: value,
  });
  ctx.set("value", value);
};

const cas = async (ctx: restate.ObjectContext, request: { expected: object; newValue: object }) => {
  const currentValue = await ctx.get("value");
  if (currentValue === request.expected) {
    ctx.console.log({
      msg: "Updating value",
      oldValue: currentValue,
      expected: request.expected,
      newValue: request.newValue,
    });
    ctx.set("value", request.newValue);
  } else {
    ctx.console.log({ msg: "Precondition failed", currentValue, expected: request.expected });
    throw new restate.TerminalError("Precondition failed", { errorCode: 412 });
  }
};

const clear = async (ctx: restate.ObjectContext) => {
  ctx.clear("value");
};

restate
  .endpoint()
  .bind(
    restate.object({
      name: "Register",
      handlers: { get, set, cas, clear },
    }),
  )
  .listen(9080);
