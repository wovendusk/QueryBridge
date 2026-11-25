package com.querybridge.cli.config;

import org.springframework.context.annotation.Configuration;

// ============================================================================
// DefaultCommandConfig — an EMPTY configuration class; a leftover.
// (guide step 28 — 10 seconds)
//
// The comment inside tells the story: there used to be "default routing" —
// presumably, typing a bare sentence like `how many users?` (without the
// `ask` prefix) would be routed to the ask command automatically. That was
// removed "for compatibility" (probably because intercepting unknown input
// fought with how Spring Shell parses commands), so now you must type
// `ask <question>` explicitly.
//
// What's left does nothing: a @Configuration with no @Bean methods registers
// one empty bean and that's it. It can be deleted without any effect. It's
// kept in this guide only so the docs mirror every file in the project.
//
// It is most likely also why QueryBridgeCommands.processQuestion() is PUBLIC
// and separate from ask(): it looks like the method a default router would
// have called. (That's my inference from the code; git history only has one
// commit, so I can't confirm it.)
// ============================================================================
@Configuration
public class DefaultCommandConfig {
    // Default routing removed for compatibility.
    // Use "ask <question>" explicitly.
}
