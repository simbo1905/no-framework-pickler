package io.github.simbo1905;

import io.github.simbo1905.no.framework.RecordPicklerTests;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.EngineFilter;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

import java.io.PrintWriter;
import java.util.Objects;
import java.util.stream.Collectors;

/// Unfortunately, this class is necessary to run tests in the intellij debugger as jquik will run exclusively
/// whereas when running mvn verify it all works properly.
public class DebuggerRunTests {
  public static void main(String[] args) {
    LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request()
        .selectors(DiscoverySelectors.selectClass(BaselineTests.class),
            DiscoverySelectors.selectClass(RecordPicklerTests.class),
            DiscoverySelectors.selectClass(TreeDemo.class),
            DiscoverySelectors.selectClass(io.github.simbo1905.no.framework.TreeTypeExprTests.class),
            DiscoverySelectors.selectClass(io.github.simbo1905.no.framework.EnumConstantTests.class),
            DiscoverySelectors.selectClass(io.github.simbo1905.no.framework.ArrayInternalTests.class),
            DiscoverySelectors.selectClass(io.github.simbo1905.no.framework.ZigZagTests.class),
            DiscoverySelectors.selectClass(io.github.simbo1905.no.framework.NestedMapTests.class),
            DiscoverySelectors.selectClass(io.github.simbo1905.no.framework.RefValueTests.class),
            DiscoverySelectors.selectClass(io.github.simbo1905.no.framework.BackwardsCompatibilityTests.class)
        )
        .filters(EngineFilter.includeEngines("junit-jupiter"))
        .build();

    // Create the launcher and listener
    Launcher launcher = LauncherFactory.create();
    SummaryGeneratingListener listener = new SummaryGeneratingListener();

    // Execute the tests
    launcher.execute(request, listener);

    // Print the summary to the console
    try (PrintWriter writer = new PrintWriter(System.out, true)) {
      listener.getSummary().printTo(writer);
    }

    if (!listener.getSummary().getFailures().isEmpty()) {
      for (var failure : listener.getSummary().getFailures()) {
        System.err.println("Test failed: " + failure.getException().getMessage());
      }
      throw new AssertionError("Some tests failed: " + listener.getSummary().getFailures()
          .stream().map(Objects::toString).collect(Collectors.joining(",")));
    }
  }
}
