# Do not change the public interfaces: the new calls are extension methods

Status: **accepted on 2026-11-01** at the request of the requester. It supersedes [0003](0003-public-api-as-interface-members.md).

## Context and Problem Statement

Decision 0003 exposed the new features as members of the public interfaces (`IPulsarClient`, `IConsumer`, `IProducer`, `IMessage`,
the builders), "following the repo precedent" (`AllowOutOfOrderDeliver`), and called it out in the CHANGELOG as a breaking change for
custom implementations. By the end of WP11 the work had added **40 members to 9 public interfaces**. A package validation of the branch
against `master` (see [wp12](../implementation-notes/wp12-non-breaking-public-api.md), section 9) reported them all as `CP0006 Cannot add
interface member`.

The requester's requirement: *"this work should not be a breaking change in any way, breaking public api is a breaking change"*, and
*"can we avoid that and still achieve the same result?"*

A new member on a public interface breaks, at compile time and at run time, every class that implements the interface outside of DotPulsar:
a decorator, a hand-written fake, a wrapper that a library of a user provides. It does not break code that only calls the interface,
and it does not break mocks that a framework generates.

## Considered Options

1. **Interface members** (decision 0003). Rejected: it is the breaking change.
2. **Default interface members** (a member with a body in the interface). Rejected: DotPulsar targets `netstandard2.0`, where the runtime has
   no default interface members (the compiler refuses: CS8701), so the API would differ per target framework and the `netstandard2.0` build
   could not have the new calls at all.
3. **New derived interfaces** (`IConsumer2 : IConsumer`) that the factories return. Rejected: the factories are members of released interfaces
   (`IConsumerBuilder<T>.Create()`, `IPulsarClientBuilder.Build()`), and changing their return type is itself a breaking change; the user would
   have to cast.
4. **Extension methods with the names, the parameters and the documentation of the members, which call an optional capability** that DotPulsar's
   own implementations have. **Chosen.**

## Decision Outcome

Chosen option: **4**.

- **No public interface of `master` gets a member**, now or later. `PublicApiTests` fails when one does (it holds the members of every released
  interface of `master`).
- Every call that was an interface member is an extension method in `DotPulsar.Extensions` with the same name and parameters, so the call
  sites do not change (they need `using DotPulsar.Extensions;`, which the examples and every existing extension already need).
- The extension method reaches the implementation in one of two ways, by `ExtendedApi.Require<T>`:
  - a **capability interface** for the types that the extension cannot name because they are generic: `IExtendedConsumer`, `IExtendedProducer`,
    `IExtendedPulsarClient`, `IExtendedMessage`, in `DotPulsar.Internal.Abstractions`, implemented by `Consumer<T>`, `SubConsumer<T>`,
    `Producer<T>`, `PulsarClient` and `Message<T>`;
  - the **builder class** that DotPulsar creates (`ConsumerBuilder<T>`, `ProducerBuilder<T>`, `ReaderBuilder<T>`, `MessageBuilder<T>`,
    `PulsarClientBuilder`), whose public methods already exist.
- An implementation of an interface that DotPulsar did not create does not have the capability: the extension method throws
  `NotSupportedException` ("'X' is not supported by 'T'. It is only supported by the implementations that DotPulsar creates."). The exception
  is `IMessage.GetEncryptionContext()`, which returns `null`: a message that DotPulsar did not create has no encryption context, and a getter
  must not throw.
- `IMessage.EncryptionContext` (a property) is the method `GetEncryptionContext()`. An extension property needs C# 14 at the call site.
- The capability interfaces are in the `Internal` namespace on purpose. Promoting one to `DotPulsar.Abstractions` later (so that a decorator
  can opt in) is additive; demoting it would not be.

## Consequences

- Positive: nothing that compiles against `master` stops compiling or running; no CHANGELOG entry about a breaking change; the call syntax of the
  features is the same as before.
- Negative: the members are not discoverable on the interface (IntelliSense shows them on a variable of the interface type only when
  `DotPulsar.Extensions` is imported); a mock of `IConsumer` needs `Substitute.For<IConsumer, IExtendedConsumer>()` to take part in the new
  calls; a decorator of `IConsumer` does not forward the new calls (they throw `NotSupportedException`) until it implements the capability.
- A feature that needs a new member in the future follows [the rule in wp12, section 2](../implementation-notes/wp12-non-breaking-public-api.md#2-the-rule-for-new-work).
