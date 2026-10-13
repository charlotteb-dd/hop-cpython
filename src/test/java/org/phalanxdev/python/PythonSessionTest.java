package org.phalanxdev.python;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.row.RowMeta;
import org.apache.hop.core.row.value.ValueMetaFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.phalanxdev.python.PythonSession.PythonVariableType;
import org.phalanxdev.python.PythonSession.RowMetaAndRows;

class PythonSessionTest {
	
	private static final String REQUESTER = "JUnitTestRequester";
    private static final String ENV_NAME = "test_env";
    private static String pythonPath;
    
	@BeforeAll
    public static void initHopEnvironment() throws Exception {
        // 1. Initialize Hop plugin registry for ValueMeta types
        HopClientEnvironment.init();

        // 2. Resolve Python executable (prioritize CLI/JVM property, then venv, then fallback)
        String propPath = System.getProperty("python.test.path");
        if (propPath != null && new File(propPath).exists()) {
            pythonPath = propPath;
        } else {
            // Default to your local venv path or standard python binary
            File venvPython = new File("/home/cbuat/work/hop-cpython/hop-cpython-env/bin/python3");
            if (venvPython.exists()) {
                pythonPath = venvPython.getAbsolutePath();
            } else {
                pythonPath = "python3";
            }
        }
    }
	
	@BeforeEach
    public void setUp() throws Exception {
        boolean initialized = PythonSession.initSession(pythonPath, null, null);
        assertTrue(initialized, "Python session failed to initialize. Check python path and dependencies.");
    }

    @AfterEach
    public void tearDown() {
        PythonSession.releaseSession(REQUESTER);
    }
    
    @Test
    @DisplayName("Verify DataFrame transfer, isolated execution, and type inspection")
    public void testDataFrameAndVariableScoping() throws Exception {
        PythonSession session = PythonSession.acquireSession(REQUESTER);
        assertNotNull(session, "Session acquisition returned null");
        
        // Ensure all operations use the isolated environment
        session.setEnvName(ENV_NAME);

        // 1. Create sample metadata and row data (Long for Hop Integer)
        IRowMeta rowMeta = new RowMeta();
        rowMeta.addValueMeta(ValueMetaFactory.createValueMeta("Field1", IValueMeta.TYPE_INTEGER));
        rowMeta.addValueMeta(ValueMetaFactory.createValueMeta("Field2", IValueMeta.TYPE_NUMBER));
        rowMeta.addValueMeta(ValueMetaFactory.createValueMeta("Field3", IValueMeta.TYPE_STRING));
        rowMeta.addValueMeta(ValueMetaFactory.createValueMeta("Field4", IValueMeta.TYPE_BOOLEAN));
        rowMeta.addValueMeta(ValueMetaFactory.createValueMeta("Field5", IValueMeta.TYPE_DATE));
        rowMeta.addValueMeta(ValueMetaFactory.createValueMeta("Field6", IValueMeta.TYPE_TIMESTAMP));
        rowMeta.addValueMeta(ValueMetaFactory.createValueMeta("NullField", IValueMeta.TYPE_STRING));

        Object[] rowData = {
                22L,
                300.22,
                "Hello bob",
                false,
                new Date(),
                new Timestamp(System.currentTimeMillis()),
                null
        };

        List<Object[]> rows = new ArrayList<>();
        rows.add(rowData);

        // 2. Send DataFrame to Python
        session.rowsToPythonDataFrame(rowMeta, rows, "In");

        // 3. Verify variable was created inside test_env
        assertTrue(session.checkIfPythonVariableIsSet("In"), "Variable 'In' should be set in environment: " + ENV_NAME);

        // 4. Retrieve DataFrame back from Python
        RowMetaAndRows result = session.rowsFromPythonDataFrame("In", false);
        assertNotNull(result);
        assertEquals(1, result.rows.length, "Expected exactly 1 returned row");
        assertEquals(7, result.rowMeta.size(), "Expected 7 metadata columns returned");

        // 5. Execute script using bound envName
        session.executeScript(ENV_NAME, "x = 100\n");
        assertTrue(session.checkIfPythonVariableIsSet("x"), "Variable 'x' should exist in environment: " + ENV_NAME);

        // 6. Verify variable type lookup
        PythonVariableType type = session.getPythonVariableType("x");
        assertEquals(PythonVariableType.Unknown, type, "Variable 'x' should be identified as unknown");
    }
	
    
    @Test
    @DisplayName("Verify that intentional socket death triggers reset and allows recovery")
    public void testServerCrashAndRecovery() throws Exception {
        PythonSession session = PythonSession.acquireSession(REQUESTER);
        session.setEnvName(ENV_NAME);

        assertThrows(HopException.class, () -> {
            session.executeScript(ENV_NAME, "import sys\nsys.exit(1)\n");
        }, "Script execution must throw an exception when sys.exit terminates the process");

        PythonSession.releaseSession(REQUESTER);

        assertFalse(session.isSessionAlive(), "Session must not be alive after server process exit");

        boolean reinitOk = PythonSession.initSession(pythonPath, null, null);
        assertTrue(reinitOk, "Re-initialization must succeed after server termination");

        PythonSession recoveredSession = PythonSession.acquireSession(REQUESTER);
        recoveredSession.setEnvName(ENV_NAME);
        assertTrue(recoveredSession.isSessionAlive(), "New session must be connected and active");

        assertDoesNotThrow(() -> {
            recoveredSession.executeScript(ENV_NAME, "y = 42\n");
        }, "Recovered server must execute commands cleanly without Broken Pipe");

        assertTrue(recoveredSession.checkIfPythonVariableIsSet("y"), "Variable 'y' must exist in recovered session");
    }
}