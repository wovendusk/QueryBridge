package com.querybridge.cli.config;   // small Spring configuration classes for the CLI

import org.jline.utils.AttributedString;   // JLine: a string that can carry colours/bold/etc.
import org.jline.utils.AttributedStyle;    // JLine: the style (colour, bold...) for an AttributedString
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.shell.jline.PromptProvider;   // Spring Shell's hook for "what does the prompt look like?"

// ============================================================================
// ShellConfig — customises the shell prompt. (guide step 27)
//
// JLINE: Spring Shell doesn't read the terminal itself. It uses JLine, a Java
// library for rich terminal input (line editing with arrow keys, history with
// up/down, tab completion, colours). JLine's AttributedString is a string
// plus styling information.
//
// HOW THE OVERRIDE WORKS
//   Spring Shell asks the container for a PromptProvider bean every time it
//   draws the prompt. It ships a default one (prompt "shell:>"), but the
//   default is registered as "only if the user didn't define one"
//   (@ConditionalOnMissingBean). By defining our own PromptProvider bean, we
//   replace it. This "define a bean of the same type to replace Boot's
//   default" trick is one of the most common Spring Boot customisation
//   patterns — remember it.
// ============================================================================
@Configuration
public class ShellConfig {

    // ------------------------------------------------------------------------
    // promptProvider()
    //
    // PromptProvider is a FUNCTIONAL interface (one abstract method:
    // AttributedString getPrompt()), so we can return a lambda instead of
    // writing a class. The lambda is called each time the prompt is shown,
    // which would let you make it dynamic (e.g. show the connected database
    // name) — here it's always the same text.
    // ------------------------------------------------------------------------
    @Bean
    public PromptProvider promptProvider() {
        // "querybridge> " in the terminal's default style (no colour).
        // e.g. AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN) would make it green.
        return () -> new AttributedString("querybridge> ", AttributedStyle.DEFAULT);
    }
}
