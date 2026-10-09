package org.phalanxdev.python;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.phalanxdev.python.PythonSession.RowMetaAndRows;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;

import org.apache.hop.core.row.IRowMeta;
import org.apache.hop.core.row.IValueMeta;

@ExtendWith(MockitoExtension.class)
class ServerUtilsTest {
	@Mock
    private IRowMeta rowMeta;

    @Mock
    private IValueMeta valueMeta1;

    @Mock
    private IValueMeta valueMeta2;
    
    private static final String MISSING_VALUE = "?";
    
//    @BeforeEach
//    void setUp() {
//        // Common setup can go here if needed
//    }
	
    @Test
    void csvToRows_ParsesValidCsvSuccessfully() throws IOException {
        String csv = "value1,123#||#value2,456";
        int numRows = 2;

        when(rowMeta.size()).thenReturn(2);
        when(rowMeta.getValueMeta(0)).thenReturn(valueMeta1);
        when(rowMeta.getValueMeta(1)).thenReturn(valueMeta2);
        
        when(valueMeta1.getType()).thenReturn(IValueMeta.TYPE_STRING); // Assuming 1 = String
        when(valueMeta2.getType()).thenReturn(IValueMeta.TYPE_NUMBER); // Assuming 2 = number

        RowMetaAndRows result = ServerUtils.csvToRows(csv, rowMeta, numRows);

        assertNotNull(result);
        assertEquals(rowMeta, result.rowMeta);
        assertEquals(2, result.rows.length);
        assertNotNull(result.rows[0]);
        assertNotNull(result.rows[1]);
    }
    
    @Test
    void csvToRows_SkipsEmptyLines() throws IOException {
        String csv = "#||##||#value1,123"; // Two empty lines, one valid line
        int numRows = 1;

        when(rowMeta.size()).thenReturn(2);
        when(rowMeta.getValueMeta(0)).thenReturn(valueMeta1);
        when(rowMeta.getValueMeta(1)).thenReturn(valueMeta2);

        RowMetaAndRows result = ServerUtils.csvToRows(csv, rowMeta, numRows);

        assertNotNull(result.rows[0]);
        assertEquals(1, result.rows.length); // Only 1 row should be processed
    }
    
    @Test
    void csvToRows_IgnoresMissingValues() throws IOException {
        String csv = "value1," + MISSING_VALUE; 
        int numRows = 1;

        when(rowMeta.size()).thenReturn(2);
        when(rowMeta.getValueMeta(0)).thenReturn(valueMeta1);

        RowMetaAndRows result = ServerUtils.csvToRows(csv, rowMeta, numRows);

        assertNotNull(result.rows[0]);
        assertNull(result.rows[0][1]);
    }
    
    @Test
    void csvToRows_ParseCarriageReturnsAndLineFeeds() throws IOException {
        String csv = "val\nue1,val\rue2";
        int numRows = 1;

        when(rowMeta.size()).thenReturn(2);
        when(rowMeta.getValueMeta(0)).thenReturn(valueMeta1);
        when(valueMeta1.getType()).thenReturn(IValueMeta.TYPE_STRING); 
        when(rowMeta.getValueMeta(1)).thenReturn(valueMeta2);
        when(valueMeta2.getType()).thenReturn(IValueMeta.TYPE_STRING);

        RowMetaAndRows result = ServerUtils.csvToRows(csv, rowMeta, numRows);

        assertNotNull(result);
        assertEquals( "Should have parsed exactly 1 row", 1, result.rows.length);
        assertNotNull("Row data should not be null", result.rows[0]);
        assertEquals("val\nue1", result.rows[0][0]);
        assertEquals("val\rue2", result.rows[0][1]);
    }
    
    @Test
    void csvToRows_ThrowsIOExceptionOnParseFailure() {
        String csv = "badValue,123";
        int numRows = 1;
        
        when(rowMeta.size()).thenReturn(2);
        
        when(rowMeta.getValueMeta(0)).thenReturn(valueMeta1);
        when(valueMeta1.getType()).thenReturn(IValueMeta.TYPE_STRING); 
        
        when(rowMeta.getValueMeta(1)).thenReturn(valueMeta2);
        when(valueMeta2.getType()).thenReturn(IValueMeta.TYPE_DATE); 
        when(valueMeta2.getName()).thenReturn("FailingColumnName");

        IOException exception = assertThrows(IOException.class, () -> {
            ServerUtils.csvToRows(csv, rowMeta, numRows);
        });
        System.out.println(exception);
        assertTrue(exception.getMessage().contains("Failed to parse column 'FailingColumnName' at row 1"));
    }
}